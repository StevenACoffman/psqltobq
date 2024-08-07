// Package ds provides general datastructures of general use
package ds

import "time"

// ImportTableInfo provides all the accumulated information associated with
// incrementally exporting from AlloyDB (PostgreSQL),
// and importing to BigQuery.
type ImportTableInfo struct {
	TableName                 string
	LocalFileName             string
	LocalScratchDirectory     string
	LocalFullFilePath         string
	BQProject                 string
	PKs                       []string
	ColNames                  []string
	PSQLColumnTypes           map[string]string
	SourceColumnTypes         map[string]string
	TargetColumnTypes         map[string]string
	ObjectName                string
	TempDataset               string
	TempTableName             string
	FinalDataset              string
	GCSFolder                 string
	GCSBucket                 string
	CSVHeaders                []string
	FQBQTempTableName         string
	FQBQFinalDataSetDestTable string
	LastUpdatedStr            string
	LastUpdatedTSStr          string
	LastUpdated               time.Time
}
