package postgres

import (
	"fmt"
	"strings"
)

func CreateRow(databaseName string, tableName string, data map[string]interface{}) error {
	log := CreateLogger("row.CreateRow")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return err
	}
	defer db.Close()

	var columns []string
	var placeholders []string
	var values []any
	ii := 0
	for key, value := range data {
		if value == nil || value == "" || value == 0 || value == "-" {
			continue
		}

		columns = append(columns, fmt.Sprintf("%#v", key))
		placeholders = append(placeholders, fmt.Sprintf("$%d", ii+1))
		values = append(values, value)
		ii++
	}

	cmd := fmt.Sprintf("INSERT INTO %#v (%s) VALUES (%s)", tableName, strings.Join(columns, ","), strings.Join(placeholders, ","))
	_, err = db.Exec(cmd, values...)
	if err != nil {
		log.WithError(err).Fatalf("db.Exec has failed.\ncmd: %v\n\ncolumns: %v \nvalues: %v\ndata: %v", cmd, columns, values, data)
		return err
	}

	return nil
}

func DeleteRowById(databaseName string, tableName string, rowId int) error {
	log := CreateLogger("row.DeleteRowById")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return err
	}
	defer db.Close()

	cmd := fmt.Sprintf(`DELETE from %#v WHERE id=%d`, tableName, rowId)
	_, err = db.Exec(cmd)
	if err != nil {
		log.WithError(err).Error("db.Exec - delete has failed. \ncmd: %s", cmd)
		return err
	}

	return nil
}

func GetRowFromId[T any](databaseName string, tableName string, recorId int) (T, error) {
	log := CreateLogger("row.GetRowFromId")
	var record T

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return record, err
	}
	defer db.Close()

	cmd := fmt.Sprintf(`SELECT * FROM %#v WHERE id=%d`, tableName, recorId)
	rows, err := db.Query(cmd)
	if err != nil {
		log.WithError(err).Error("db.Query has failed. \ncmd: %s", cmd)
		return record, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&record)
		if err != nil {
			log.WithError(err).Error("rows.Scan has failed")
			return record, err
		}
	}

	return record, nil
}

func GetRows[T any](databaseName string, tableName string) ([]T, error) {
	log := CreateLogger("row.GetRows")
	var records []T

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return records, err
	}
	defer db.Close()

	cmd := fmt.Sprintf(`SELECT * FROM %#v`, tableName)
	rows, err := db.Query(cmd)
	if err != nil {
		log.WithError(err).Error("db.Query has failed")
		return records, err
	}
	defer rows.Close()

	for rows.Next() {
		var record T
		err := rows.Scan(&record)
		if err != nil {
			log.WithError(err).Error("rows.Scan has failed")
			return records, err
		}
		records = append(records, record)
	}

	return records, nil
}

func UpdateRow(databaseName string, tableName string, rowId int, data map[string]interface{}) error {
	log := CreateLogger("row.UpdateRow")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return err
	}
	defer db.Close()

	var columnsStr string
	count := len(data)
	for key, value := range data {
		count--

		valStr, ok := value.(string)
		if ok {
			columnsStr += fmt.Sprintf(`%#v='%s'`, key, valStr)
		}
		if !ok {
			columnsStr += fmt.Sprintf(`%#v=%v`, key, value)
		}

		if count > 0 {
			columnsStr += ", "
		}
	}

	cmd := fmt.Sprintf(`UPDATE %#v SET %s WHERE id=%d`, tableName, columnsStr, rowId)
	log.Debugf("cmd: %s", cmd)

	_, err = db.Exec(cmd)
	if err != nil {
		log.WithError(err).Error("db.Exec has failed")
		return err
	}

	return nil
}

func DoesRowExistsById(databaseName string, tableName string, rowId int) (bool, error) {
	log := CreateLogger("row.DoesRowExistsById")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return false, err
	}
	defer db.Close()

	cmd := fmt.Sprintf(`SELECT EXISTS (SELECT * FROM %#v WHERE id=%d)`, tableName, rowId)
	log.Debugf("cmd: %s", cmd)

	var exists bool
	err = db.QueryRow(cmd).Scan(&exists)
	if err != nil {
		log.WithError(err).Error("row.Scan has failed")
		return false, err
	}

	return exists, nil
}

func DoesRowExists(databaseName string, tableName string, searchParams map[string]any) (bool, error) {
	log := CreateLogger("row.DoesRowExists")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return false, err
	}
	defer db.Close()

	var valuesStr string
	count := len(searchParams)
	for key, value := range searchParams {
		count--

		valueInt, ok := value.(string)
		if ok {
			valuesStr += fmt.Sprintf(`%s = '%s'`, key, valueInt)
		}
		if !ok {
			valuesStr += fmt.Sprintf(`%s = %#v`, key, value)
		}

		if count > 0 {
			valuesStr += " AND "
		}
	}

	cmd := fmt.Sprintf(`SELECT EXISTS (SELECT * FROM %#v WHERE %s)`, tableName, valuesStr)
	log.Debugf("cmd: %s", cmd)

	var exists bool
	err = db.QueryRow(cmd).Scan(&exists)
	if err != nil {
		log.WithError(err).Error("row.Scan has failed")
		return false, err
	}

	return exists, nil
}

func GetRowIdFromSearchParams(databaseName string, tableName string, searchParams map[string]any) (int64, error) {
	log := CreateLogger("row.GetRowIdFromSearchParams")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return 0, err
	}
	defer db.Close()

	var condition []string
	var values []any
	ii := 0
	for key, value := range searchParams {
		if value == nil || value == "" || value == 0 || value == "-" {
			continue
		}

		condition = append(condition, fmt.Sprintf("%s = $%d", key, ii+1))
		values = append(values, value)
		ii++
	}

	var id int64
	cmd := fmt.Sprintf("SELECT id FROM %#v WHERE %s", tableName, strings.Join(condition, " AND "))
	err = db.QueryRow(cmd, values...).Scan(&id)
	if err != nil {
		log.WithError(err).Errorf("row.Scan has failed.\ncmd: %s\n values:%v", cmd, values)
		return 0, err
	}

	return id, nil
}

func GetRowFromSearchParams[T any](databaseName string, tableName string, searchParams map[string]any) (T, error) {
	log := CreateLogger("row.GetRecordFromSearchParams")
	var record T

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return record, err
	}
	defer db.Close()

	var condition []string
	var values []any
	ii := 0
	for key, value := range searchParams {
		if value == nil || value == "" || value == 0 || value == "-" {
			continue
		}

		condition = append(condition, fmt.Sprintf("%s = $%d", key, ii+1))
		values = append(values, value)
		ii++
	}

	cmd := fmt.Sprintf("SELECT * FROM %#v WHERE %s", tableName, strings.Join(condition, " AND "))
	err = db.QueryRow(cmd, values...).Scan(&record)
	if err != nil {
		log.WithError(err).Errorf("row.Scan has failed.\ncmd: %s\n values:%v", cmd, values)
		return record, err
	}

	return record, nil
}

func GetRowsFromSearchParams[T any](databaseName string, tableName string, searchParams map[string]any) ([]T, error) {
	log := CreateLogger("row.GetRowsFromSearchParams")
	var records []T

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return records, err
	}

	var condition []string
	var values []any
	ii := 0
	for key, value := range searchParams {
		if value == nil || value == "" || value == 0 || value == "-" {
			continue
		}

		condition = append(condition, fmt.Sprintf("%s = $%d", key, ii+1))
		values = append(values, value)
		ii++
	}

	cmd := fmt.Sprintf("SELECT * FROM %#v WHERE %s", tableName, strings.Join(condition, " AND "))
	rows, err := db.Query(cmd)
	if err != nil {
		log.WithError(err).Errorf("db.Query has failed. \ncmd: %s\n values:%v", cmd, values)
		return records, err
	}
	defer rows.Close()

	for rows.Next() {
		var record T
		err := rows.Scan(&record)
		if err != nil {
			log.WithError(err).Errorf("rows.Scan has failed. \ncmd: %s\n values:%v", cmd, values)
			return records, err
		}
		records = append(records, record)
	}

	return records, nil
}
