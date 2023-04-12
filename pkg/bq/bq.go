package bq

import (
	"context"
	"fmt"
	"github.com/StevenACoffman/psqltobq/pkg/ds"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"

	"github.com/StevenACoffman/anotherr/errors"
)

func GetLast(
	ctx context.Context,
	client *bigquery.Client,
	dataset string,
	tableName string,
) ([][]bigquery.Value, error) {
	q := `SELECT ` +
		`FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',
		CAST(MAX(last_updated) AS TIMESTAMP))
	AS max_date
	FROM ` + "`khanacademy.org:deductive-jet-827`" + "." + dataset + "." + tableName + ";"
	return performBigQuery(ctx, client, q)
}

func GetColumnTypes(
	ctx context.Context,
	client *bigquery.Client,
	dataset string,
	tableName string,
) (map[string]string, error) {
	columnTypes := make(map[string]string)

	q := fmt.Sprintf(`SELECT `+
		`column_name, data_type FROM `+
		"`khanacademy.org:deductive-jet-827`"+".%s.INFORMATION_SCHEMA.COLUMNS "+
		"WHERE table_name='%s'", dataset, tableName)

	rows, err := performBigQuery(ctx, client, q)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		columnName := fmt.Sprint(row[0])
		dataType := fmt.Sprint(row[1])
		columnTypes[columnName] = dataType
	}

	return columnTypes, nil
}

func performBigQuery(
	ctx context.Context,
	client *bigquery.Client,
	q string,
) ([][]bigquery.Value, error) {
	query := client.Query(q)
	iter, err := query.Read(ctx) // *bigquery.RowIterator
	if err != nil {
		return nil, errors.Wrap(err, "Unable to read BigQuery query "+q)
	}

	var rows [][]bigquery.Value

	for {
		var row []bigquery.Value
		err = iter.Next(&row)
		if errors.Is(err, iterator.Done) {
			return rows, nil
		}
		if err != nil {
			return nil, errors.Wrap(err, "Unable to get next BQ row")
		}
		rows = append(rows, row)
	}
}

func ImportCSVAutodetectSchema(
	ctx context.Context,
	client *bigquery.Client,
	bucket string,
	info *ds.ImportTableInfo,
) error {
	uri := fmt.Sprintf("gs://%s/%s", bucket, info.ObjectName)
	gcsRef := bigquery.NewGCSReference(uri)
	gcsRef.SourceFormat = bigquery.CSV
	gcsRef.FieldDelimiter = "^"
	gcsRef.IgnoreUnknownValues = true // should not happen
	// gcsRef.MaxBadRecords = 0
	// gcsRef.AllowJaggedRows = true
	gcsRef.Schema = bigquery.Schema{}

	for _, colName := range info.CSVHeaders {
		colType, found := info.PSQLColumnTypes[colName]
		if !found {
			return fmt.Errorf("no PSQL type found for %s", colName)
		}
		bqType, ok := fieldMap[strings.ToUpper(colType)]
		if !ok {
			return fmt.Errorf("No BQ type found for column: %s PSQL type: %s"+colName, colType)
		}
		fs := bigquery.FieldSchema{
			Name: colName,
			Type: bqType,
		}
		gcsRef.Schema = append(gcsRef.Schema, &fs)
	}
	gcsRef.SkipLeadingRows = 1

	loader := client.Dataset(info.TempDataset).Table(info.TempTableName).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteTruncate

	job, err := loader.Run(ctx)
	if err != nil {
		return errors.Wrap(err, "BQ CSV loader Run failed")
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return errors.Wrap(err, "BQ CSV Load Wait failed")
	}

	if status != nil && status.Err() != nil {
		return errors.Wrap(status.Err(), "NQ load job completed with error")
	}
	return nil
}

func GetLastModifiedForTable(
	ctx context.Context,
	client *bigquery.Client,
	info *ds.ImportTableInfo,
) error {
	rows, err := GetLast(ctx, client, info.FinalDataset, info.TableName)
	if err != nil {
		return errors.Wrap(
			err,
			"Unable to GetLast for table "+info.TableName,
		)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return errors.New("No Get LastModified Date")
	}

	info.LastUpdatedStr = fmt.Sprint(rows[0][0])
	if info.LastUpdatedStr == "" {
		return errors.New("table had empty lastupdated string")
	}

	layout := "2006-01-02 15:04:05"
	info.LastUpdated, err = time.Parse(layout, info.LastUpdatedStr)
	if err != nil {
		return errors.Wrap(
			err,
			"Unable to parse date "+info.LastUpdatedStr,
		)
	}

	info.LastUpdatedTSStr = info.LastUpdated.Format("20060102_1504")
	return nil
}

func MakeAndPerformMerge(
	ctx context.Context,
	logger *zap.Logger,
	client *bigquery.Client,
	info *ds.ImportTableInfo,
) error {
	err := ImportCSVAutodetectSchema(
		ctx,
		client,
		"ephemeral.khanacademy.org",
		info,
	)
	if err != nil {
		return errors.Wrap(err, "Unable to import CSV into BQ")
	}

	info.TargetColumnTypes, err = GetColumnTypes(ctx, client, info.TempDataset, info.TableName)
	if err != nil {
		return errors.Wrap(err, "Unable to get BQ Column Types for Final Destination")
	}

	info.SourceColumnTypes, err = GetColumnTypes(ctx, client, info.TempDataset, info.TempTableName)
	if err != nil {
		return errors.Wrap(err, "Unable to get BQ Column Types for Temp BQ table")
	}
	logger.Info(
		fmt.Sprint(
			"Source Table:"+info.FQBQTempTableName,
			"Primary Keys:",
			strings.Join(info.PKs, ","),
		),
	)
	mergeQuery := CreateMergeQuery(info)
	logger.Info(mergeQuery)
	_, err = performBigQuery(ctx, client, mergeQuery)
	if err != nil {
		return errors.Wrap(err, "Unable to perform MergeQuery")
	}
	return nil
}

func CreateMergeQuery(info *ds.ImportTableInfo) string {
	nonPks := setSubstraction(info.ColNames, info.PKs)
	var valCols []string
	for _, v := range nonPks {
		if info.SourceColumnTypes[v] == info.TargetColumnTypes[v] {
			valCols = append(valCols, fmt.Sprintf("T.%s = S.%s", v, v))
		} else {
			valCols = append(valCols, fmt.Sprintf("T.%s = CAST(S.%s AS %s)", v, v, info.TargetColumnTypes[v]))
		}
	}

	var joinCols []string
	for _, v := range info.PKs {
		joinCols = append(joinCols, fmt.Sprintf("T.%s = S.%s", v, v))
	}
	joinColsStr := strings.Join(joinCols, " AND ")

	mergeQuery := fmt.Sprintf(`MERGE INTO %s T
				        USING %s S
				        ON (%s)
				        WHEN MATCHED AND S.last_updated >= '%s' THEN
				            UPDATE SET %s
				        WHEN NOT MATCHED THEN
				            %s;`, info.FQBQTempDataSetDestTable, info.FQBQTempTableName, joinColsStr, info.LastUpdatedStr, strings.Join(valCols, ",\n"), createInsert(info.ColNames))
	return mergeQuery
}

func setSubstraction(superset, subset []string) []string {
	set := make(map[string]bool)
	for _, value := range subset {
		set[value] = true
	}
	var result []string
	for _, value := range superset {
		if found := set[value]; !found {
			result = append(result, value)
		}
	}

	return result
}

// createInsert will generate an insert statement like:
// `INSERT (
//
//	id,
//	latest,
//	history)
//
// VALUES (
//
//	staging.id,
//	staging.latest,
//	staging.history)`
func createInsert(colNames []string) string {
	var sourceCols []string
	for _, v := range colNames {
		sourceCols = append(sourceCols, fmt.Sprintf("S.%s", v))
	}
	return fmt.Sprintf(
		`INSERT (%s) VALUES (%s)`,
		strings.Join(colNames, ",\n"),
		strings.Join(sourceCols, ",\n"),
	)
}

var fieldMap = map[string]bigquery.FieldType{
	"BOOL":                        bigquery.BooleanFieldType, // alias
	"FLOAT64":                     bigquery.FloatFieldType,   // alias
	"INT64":                       bigquery.IntegerFieldType, // alias
	"STRUCT":                      bigquery.RecordFieldType,
	"DECIMAL":                     bigquery.NumericFieldType,
	"BIGDECIMAL":                  bigquery.BigNumericFieldType,
	"BIGNUMERIC":                  bigquery.BigNumericFieldType,
	"BOOLEAN":                     bigquery.BooleanFieldType,
	"BYTES":                       bigquery.BytesFieldType,
	"DATE":                        bigquery.DateFieldType,
	"DATETIME":                    bigquery.DateTimeFieldType,
	"FLOAT":                       bigquery.FloatFieldType,
	"GEOGRAPHY":                   bigquery.GeographyFieldType,
	"INTEGER":                     bigquery.IntegerFieldType,
	"INTERVAL":                    bigquery.IntervalFieldType,
	"JSON":                        bigquery.JSONFieldType,
	"NUMERIC":                     bigquery.NumericFieldType,
	"RECORD":                      bigquery.RecordFieldType,
	"STRING":                      bigquery.StringFieldType,
	"TIME":                        bigquery.TimeFieldType,
	"TIMESTAMP":                   bigquery.TimestampFieldType,
	"UUID":                        bigquery.StringFieldType,   // no exact match for psql
	"TEXT":                        bigquery.StringFieldType,   // psql
	"TIMESTAMP WITHOUT TIME ZONE": bigquery.DateTimeFieldType, // no exact match for psql
	"BIGINT":                      bigquery.IntegerFieldType,  // psql
	"SMALLINT":                    bigquery.IntegerFieldType,  // psql
	"TINYINT":                     bigquery.IntegerFieldType,  // psql
	"MEDIUMINT":                   bigquery.IntegerFieldType,  // psql
	"INT":                         bigquery.IntegerFieldType,  // psql
	"REAL":                        bigquery.FloatFieldType,    // psql
	"DOUBLE PRECISION":            bigquery.FloatFieldType,    // psql
	"CHAR":                        bigquery.StringFieldType,   // psql
	"CHARACTER":                   bigquery.StringFieldType,   // psql
	"VARCHAR":                     bigquery.StringFieldType,   // psql
	"TINYTEXT":                    bigquery.StringFieldType,   // psql
	"MEDIUMTEXT":                  bigquery.StringFieldType,   // psql
	"LONGTEXT":                    bigquery.StringFieldType,   // psql
	"BYTEA":                       bigquery.BytesFieldType,    // psql
	"ENUM":                        bigquery.StringFieldType,   // this is *least* wrong? no equivalent
}
