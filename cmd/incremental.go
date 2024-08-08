package cmd

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/elliotchance/sshtunnel"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/mattn/go-isatty"
	"go.uber.org/zap"

	"github.com/StevenACoffman/anotherr/errors"
	"github.com/StevenACoffman/psqltobq/pkg/bq"
	"github.com/StevenACoffman/psqltobq/pkg/csvscan"
	"github.com/StevenACoffman/psqltobq/pkg/ds"
	"github.com/StevenACoffman/psqltobq/pkg/gcpapi"
	"github.com/StevenACoffman/psqltobq/pkg/gcs"
	"github.com/StevenACoffman/psqltobq/pkg/psql"
)

func Incremental(logger *zap.Logger) error {
	isTerminal := false
	if isatty.IsTerminal(os.Stdout.Fd()) {
		isTerminal = true
	} else if isatty.IsCygwinTerminal(os.Stdout.Fd()) {
		isTerminal = true
	}

	bqProject := getEnv("BQ_GOOGLE_CLOUD_PROJECT", "khanacademy.org:deductive-jet-827")
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, bqProject)
	if err != nil {
		return errors.Wrap(err, "Unable to open bigquery client")
	}

	defer func() {
		// if the ctx is cancelled, we still want to Close, so use Background
		_ = client.Close()
		logger.Info("bq client was closed successfully")
	}()
	credentials, credErr := gcpapi.NewCredentials(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"))
	if credErr != nil {
		return errors.Wrap(credErr, "Unable to get GCP credentials")
	}

	gcsClient, dsErr := gcpapi.NewCloudStorageClient(ctx, credentials)
	if dsErr != nil {
		return errors.Wrap(dsErr, "Unable to get NewCloudStorageClient")
	}
	dbUser := getEnv("CLOUDSQL_USER", "postgres")
	dbPass := getEnv("CLOUDSQL_PASSWORD", "")
	dbHost := getEnv("CLOUDSQL_HOST", "127.0.0.1")
	dbPort := getEnv("CLOUDSQL_PORT", "5432")
	dbName := getEnv("CLOUDSQL_DBNAME", "reports")
	dbSchema := getEnv("CLOUDSQL_SCHEMA", "public") // search_path=%s
	var connStr string
	if isTerminal {
		home, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		// gcloud compute ssh --ssh-flag=-vvv \
		// --ssh-flag="-N" --ssh-flag="-n" \
		// --ssh-flag='-L 5434:10.7.97.196:5432' \
		// --project=khan-academy --zone=us-central1-b sql-admin
		sshFilePath := filepath.Join(home, ".ssh", "google_compute_engine")
		// Setup the tunnel, but do not start it yet.
		tunnel := sshtunnel.NewSSHTunnel(
			// User and host of tunnel server, it will default to port 22
			// if not specified.
			"steve@"+getEnv("SSH_BASTION_HOST", ""), // redacted
			sshtunnel.PrivateKeyFile(sshFilePath),

			// The destination host and port of the actual server.
			"10.7.97.196:5432",

			// The local port you want to bind the remote port to.
			// Specifying "0" will lead to a random port.
			"0",
		)

		// You can provide a logger for debugging, or remove this line to
		// make it silent.
		tunnel.Log = log.New(os.Stdout, "", log.Ldate|log.Lmicroseconds)

		// Start the server in the background.
		go func() {
			tunnelErr := tunnel.Start()
			logger.Error("Unable to start tunnel", zap.Error(tunnelErr))
			os.Exit(0)
		}()
		defer tunnel.Close()
		// We will need to wait a small amount of time for it
		// to bind to the localhost dbPort
		// before you can start sending connections.
		time.Sleep(100 * time.Millisecond)
		dbPort = fmt.Sprint(tunnel.Local.Port)
		// override in case
		dbHost = "127.0.0.1"
	}
	connStr = fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s search_path=%s sslmode=disable",
		dbHost,
		dbPort,
		dbUser,
		dbPass,
		dbName,
		dbSchema,
	)

	// WARNING: This next line will print out the password, but might be useful for local debugging
	// fmt.Println(connStr)

	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		return errors.Wrap(err, "unable to open database connection")
	}
	defer func() {
		// if the ctx is cancelled, we still want to Close, so use Background
		_ = conn.Close(context.Background())
		logger.Info("Connect was closed successfully")
	}()

	pg := conn.PgConn()
	defer func() {
		// if the ctx is cancelled, we still want to Close, so use Background
		_ = pg.Close(context.Background())
		logger.Info("pg was closed successfully")
	}()
	tableList := []string{
		"agg_teacher_time_daily",
		"district_class_course",
		"student_skill_levels",
		"window_roster",
		"course_skill",
		"agg_student_sat_time_daily",
	}

	for _, tableName := range tableList {
		err = processSingleTable(
		ctx, 
		logger, 
		client, 
		conn, 
		gcsClient, 
		bqProject, 
		tableName,
		)
		if err != nil {
			return errors.Wrap(err, "table "+tableName+" processing error")
		}
	}
	return nil
}

func processSingleTable(
	ctx context.Context,
	logger *zap.Logger,
	client *bigquery.Client,
	conn *pgx.Conn,
	gcsClient *storage.Client,
	bqProject string,
	tableName string,
) error {
	info := ds.ImportTableInfo{
		TableName:    tableName,
		BQProject:    bqProject,
		GCSFolder:    "incremental",
		GCSBucket:    "ephemeral.khanacademy.org",
		FinalDataset: "reports_postgres_exported",
	}

	info.TempDataset = fmt.Sprintf("%s_temp", info.FinalDataset)

	var err error
	err = bq.GetLastModifiedForTable(
		ctx,
		client,
		&info,
	)
	if err != nil {
		return errors.Wrap(err, "Unable to get Last modified for table")
	}

	err = getPSQLInfo(ctx, conn, &info)
	if err != nil {
		return errors.Wrap(err, "Unable to get PSQL Info")
	}

	err = exporter(ctx, logger, conn, &info)
	if err != nil {
		return errors.Wrap(err, "got an error exporting!")
	}
	logger.Info("Uploading to GCS " + info.GCSBucket)

	err = gcs.UploadFile(
		ctx,
		logger,
		gcsClient,
		&info,
	)
	if err != nil {
		return errors.Wrap(err, "Got an error uploading to GCS")
	}

	err = bq.MakeAndPerformMerge(ctx, logger, client, &info)
	return errors.Wrap(err, "Got an error MakeAndPerformMerge")
}

func getPSQLInfo(
	ctx context.Context,
	conn *pgx.Conn,
	info *ds.ImportTableInfo,
) error {
	var err error
	tempTablePrefix := "temp"
	info.TempTableName = fmt.Sprintf(
		"%s_%s_%s",
		tempTablePrefix,
		info.TableName,
		info.LastUpdatedTSStr,
	)
	info.FQBQTempTableName = fmt.Sprintf(
		"`%s`.%s.%s",
		info.BQProject,
		info.TempDataset,
		info.TempTableName,
	)
	// TODO(steve): we want to change this from TempDataset to FinalDataset when releasing
	info.FQBQFinalDataSetDestTable = fmt.Sprintf(
		"`%s`.%s.%s",
		info.BQProject,
		info.TempDataset, // info.FinalDataset,
		info.TableName,
	)

	info.ColNames, info.PSQLColumnTypes, err = psql.GetPSQLColumnTypes(ctx, conn, info.TableName)
	if err != nil {
		return errors.Wrap(err, "Unable to get PSQL Column Types")
	}
	info.PKs, err = psql.GetPSQLPrimaryKeys(ctx, conn, info.TableName)
	if err != nil {
		return errors.Wrap(err, "Unable to get PSQL Primary Keys")
	}
	return err
}

// makeExportCSVFileName will add a random number to avoid collisions
// due to GCS retention policy preventing overwriting existing files
func makeExportCSVFileName(tableName string) string {
	var baseFileName string
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	extension := ".csv"
	baseFileName = fmt.Sprintf(
		"%s_%d%s",
		tableName,
		r1.Intn(10),
		extension,
	)
	return baseFileName
}

// setupTunnel will do that but not start it yet.
// equivalent to:
// gcloud compute ssh --ssh-flag=-vvv \
// --ssh-flag="-N" --ssh-flag="-n" \
// --ssh-flag='-L 5434:10.7.97.196:5432' \
// --project=khan-academy --zone=us-central1-b sql-admin
func setupTunnel(logger *zap.Logger) (*sshtunnel.SSHTunnel, error) {
	var userName string
	currentUser, err := user.Current()
	if err != nil {
		return nil, err
	}
	userName = currentUser.Name
	// TODO(steve): How do we figure out what the users GCP name (email) is?
	if strings.Contains(userName, "kath") {
		userName = "katherinephilip"
	} else {
		userName = "steve"
	}
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	sshFilePath := filepath.Join(home, ".ssh", "google_compute_engine")
	// sql-admin is 34.29.49.76
	jumpHost := "34.29.49.76"
	endpoint := fmt.Sprintf("%s@%s", userName, jumpHost)
	logger.Info("Starting tunnel to endpoint:", zap.String("endpoint", endpoint))
	tunnel := sshtunnel.NewSSHTunnel(
		// User and host of tunnel server, it will default to port 22
		// if not specified.
		endpoint,
		sshtunnel.PrivateKeyFile(sshFilePath),

		// The destination host and port of the actual server.
		//"10.7.97.196:5432", // main-production-cluster
		"10.7.97.219:5432", // main-production-read
		// The local port you want to bind the remote port to.
		// Specifying "0" will lead to a random port.
		"0",
	)
	return tunnel, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok { //nolint:ka-banned-symbol // see above
		return value
	}
	return fallback
}

// exporter will export a PSQL table to a local CSV file
func exporter(
	ctx context.Context,
	logger *zap.Logger,
	conn *pgx.Conn,
	info *ds.ImportTableInfo,
) error {
	info.LocalFileName = makeExportCSVFileName(info.TableName)
	fileCsv, err := os.Create(info.LocalFileName)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer func() {
		_ = fileCsv.Close()
		logger.Info("resource file was closed", zap.String("fileName", info.LocalFileName))
	}()
	logger.Info("opening PipeWriter for CSV file ", zap.String("fileName", info.LocalFileName))
	pipeReader, pipeWriter := io.Pipe()

	// will read + write rows as we pipe them in
	go func() {
		csvErr := csvscan.CSVScanner(
			logger,
			pipeReader,
			fileCsv,
			info.PSQLColumnTypes,
			&info.CSVHeaders,
		)
		if csvErr != nil {
			logger.Error("CSV Scanner got error", zap.Error(csvErr))
		}
	}()
	pg := conn.PgConn()
	copyToSQL := makeCopyToSQL(info)
	logger.Info(fmt.Sprint("Running:", copyToSQL))
	var res pgconn.CommandTag
	res, err = pg.CopyTo(ctx, pipeWriter, copyToSQL)
	if err != nil {
		logger.Error(fmt.Sprint("Got CopyTo error for", copyToSQL))
		logger.Error("Got CopyTo error", zap.Error(err))
		return fmt.Errorf("error exporting file: %w", err)
	}
	logger.Info(fmt.Sprint("==> export rows affected:", res.RowsAffected()))
	err = pipeWriter.Close()

	return errors.Wrap(err, "Unable to close CSV PipeWriter")
}

func makeCopyToSQL(info *ds.ImportTableInfo) string {
	queryEnd := info.TableName
	if info.LastUpdatedStr != "" {
		queryEnd = info.TableName + " WHERE last_updated >= '" + info.LastUpdatedStr + "'"
	}
	copyToSQL := fmt.Sprintf("COPY (select * from %s) TO STDOUT DELIMITER '^' CSV HEADER", queryEnd)
	return copyToSQL
}
