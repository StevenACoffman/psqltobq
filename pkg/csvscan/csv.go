package csvscan

import (
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"

	"go.uber.org/zap"

	"github.com/StevenACoffman/anotherr/errors"
)

func CSVScanner(
	logger *zap.Logger,
	r io.Reader,
	fileCsv *os.File,
	columnTypes map[string]string,
	csvHeaders *[]string,
) error {
	var headerRecord []string
	reader := csv.NewReader(r)
	reader.Comma = '^'
	csvWriter := csv.NewWriter(fileCsv)
	csvWriter.Comma = '^'
	defer csvWriter.Flush()
	var columnTypeIndex map[int]string
	var err error
	rowNum := 0
	for {
		var record []string
		record, err = reader.Read()
		rowNum++
		if errors.Is(err, io.EOF) {
			err = nil
			logger.Info("CSV Scanner read EOF", zap.Int("rowNum", rowNum))
			break
		}
		if err != nil {
			// handle the error...
			// break? continue? neither?
			logger.Error("CSV Scanner read err", zap.Int("rowNum", rowNum), zap.Error(err))
			break
		}
		if headerRecord == nil {
			headerRecord = record
			*csvHeaders = append(*csvHeaders, headerRecord...)
			columnTypeIndex = getColumnTypeIndex(headerRecord, columnTypes)
			err = csvWriter.Write(record)
			if err != nil {
				return errors.Wrap(
					err,
					fmt.Sprintf("CSV Header Write error with Rows processed:%d", rowNum),
				)
			}
		} else {
			newRecords := convertRecordForBQ(record, columnTypeIndex)
			err = csvWriter.Write(newRecords)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("CSV Write error with Rows processed: %d", rowNum))
			}
		}
		csvWriter.Flush()
	}
	return errors.Wrap(err, "csvScanner error")
}

func getColumnTypeIndex(headerRecord []string, columnTypes map[string]string) map[int]string {
	columnTypeIndex := make(map[int]string)
	for i, headerVal := range headerRecord {
		if colType, ok := columnTypes[headerVal]; ok {
			columnTypeIndex[i] = colType
		}
	}
	return columnTypeIndex
}

func convertRecordForBQ(record []string, columnTypeIndex map[int]string) []string {
	var newRecords []string
	for i, val := range record {
		if colType, ok := columnTypeIndex[i]; ok {
			newRecords = append(newRecords, psqlToBQDataType(colType, val))
		} else {
			// we have no column type? Shouldn't be possible
			newRecords = append(newRecords, val)
		}
	}
	return newRecords
}

// psqlToBQDataType maps Postgres DDL/OID types to BigQuery types, allowing us to build BigQuery
// schemas from Postgres type information.
func psqlToBQDataType(datatype, val string) string {
	switch strings.ToUpper(datatype) {
	case "DATETIME", "TIMESTAMP", "DATE":
		switch val { // BigQuery must range between 0001-01-01 to 9999-12-31.
		case "infinity":
			val = "9999-12-31"
		case "-infinity":
			val = "0001-01-01"
		}
	}
	return val
}

// MakeExportCSVFileName will add a random number to avoid collisions
// due to GCS retention policy preventing overwriting existing files
func MakeExportCSVFileName(tableName string) (string, error) {
	nBig, err := rand.Int(rand.Reader, big.NewInt(1000))
	if err != nil {
		return "", errors.Wrap(err, "Unable to generate random number")
	}
	extension := ".csv"
	return fmt.Sprintf(
		"%s_%d%s",
		tableName,
		nBig,
		extension,
	), nil
}
