package psql

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"

	"github.com/StevenACoffman/anotherr/errors"
)

func GetPSQLColumnTypes(
	ctx context.Context,
	conn *pgx.Conn,
	tableName string,
) ([]string, map[string]string, error) {
	var columnName string
	var dataType string
	columnTypes := make(map[string]string)
	var columnNames []string
	sqlStr := fmt.Sprintf(
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='%s'",
		tableName,
	)
	rows, err := conn.Query(ctx, sqlStr)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Unable to query PSQL coltypes for "+tableName)
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&columnName, &dataType)
		if err != nil {
			return columnNames, columnTypes, errors.Wrap(
				err,
				"Unable to scan PSQL coltypes for "+tableName,
			)
		}
		columnTypes[columnName] = dataType
		columnNames = append(columnNames, columnName)
	}
	return columnNames, columnTypes, errors.Wrap(err, "Unable to scan PSQL coltypes for "+tableName)
}

func GetPSQLPrimaryKeys(
	ctx context.Context,
	conn *pgx.Conn,
	tableName string,
) ([]string, error) {
	var pkName string
	var pkConstraintDef string
	sqlStr := fmt.Sprintf(
		`SELECT 
       conname AS primary_key, 
       pg_get_constraintdef(oid) 
FROM   pg_constraint 
WHERE  contype = 'p' 
AND    connamespace = 'public'::regnamespace  
AND    conrelid::regclass::text = '%s'  
ORDER  BY conrelid::regclass::text, contype DESC;`,
		tableName,
	)
	rows, err := conn.Query(ctx, sqlStr)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to Query primary key constraints for table "+tableName)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("There were no primary key constraints for table " + tableName)
	}
	err = rows.Scan(&pkName, &pkConstraintDef)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to scan primary key constraints")
	}
	pksStr := strings.TrimSuffix(strings.TrimPrefix(pkConstraintDef, "PRIMARY KEY ("), ")")
	pks := strings.Split(pksStr, ", ")
	return pks, nil
}

// Exporter will export a PSQL table to a local CSV file
func Exporter(
	ctx context.Context,
	logger *zap.Logger,
	conn *pgx.Conn,
	info *ds.ImportTableInfo,
) error {
	var err error
	info.LocalFileName, err = csvscan.MakeExportCSVFileName(info.TableName)
	if err != nil {
		return err
	}
	info.LocalFullFilePath = filepath.Join(info.LocalScratchDirectory, info.LocalFileName)

	fileCsv, err := os.Create(info.LocalFullFilePath)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer func() {
		_ = fileCsv.Close()
		logger.Info("resource file was closed", zap.String("fileName", info.LocalFullFilePath))
	}()
	logger.Info("opening PipeWriter for CSV file ", zap.String("fileName", info.LocalFullFilePath))
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
	columnNames := strings.Join(info.ColNames, ", ")
	copyToSQL := fmt.Sprintf(
		"COPY (select %s from %s) TO STDOUT DELIMITER '^' CSV HEADER",
		columnNames,
		queryEnd,
	)
	return copyToSQL
}
