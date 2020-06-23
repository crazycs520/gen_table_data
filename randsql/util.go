package randsql

import (
	"database/sql"
)

func queryResult(db *sql.DB, sql string) ([][]string, error) {
	rows, err := db.Query(sql)
	if err == nil {
		defer rows.Close()
	}
	if err != nil {
		return nil, err
	}

	// Read all rows.
	var actualRows [][]string
	for rows.Next() {
		cols, err1 := rows.Columns()
		if err1 != nil {
			return nil, err1
		}

		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err1 = rows.Scan(dest...)
		if err1 != nil {
			return nil, err1
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "NULL"
			} else {
				val := string(raw)
				result[i] = val
			}
		}

		actualRows = append(actualRows, result)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return actualRows, nil
}
