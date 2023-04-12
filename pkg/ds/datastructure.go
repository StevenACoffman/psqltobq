package ds

import "time"

type ImportTableInfo struct {
	TableName                string
	LocalFileName            string
	BQProject                string
	PKs                      []string
	ColNames                 []string
	PSQLColumnTypes          map[string]string
	SourceColumnTypes        map[string]string
	TargetColumnTypes        map[string]string
	ObjectName               string
	TempDataset              string
	TempTableName            string
	FinalDataset             string
	GCSFolder                string
	GCSBucket                string
	CSVHeaders               []string
	FQBQTempTableName        string
	FQBQTempDataSetDestTable string
	LastUpdatedStr           string
	LastUpdatedTSStr         string
	LastUpdated              time.Time
}
