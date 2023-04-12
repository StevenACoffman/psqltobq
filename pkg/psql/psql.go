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
