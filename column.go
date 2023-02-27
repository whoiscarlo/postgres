package postgres

import (
	"fmt"
	"strings"
)

type (
	Column struct {
		Name string
		Type string
	}
)

func RemoveColumn(databaseName string, tableName string, columnName string) error {
	log := CreateLogger("column.RemoveColumn")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return err
	}
	defer db.Close()

	cmd := fmt.Sprintf("ALTER TABLE '%s' DROP COLUMN %#v", tableName, columnName)
	_, err = db.Exec(cmd)
	if err != nil {
		return err
	}
	return nil
}

func AddColumn(databaseName string, tableName string, newColumns []Column) error {
	log := CreateLogger("column.AddColumn")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return err
	}
	defer db.Close()

	var values []string
	for _, each := range newColumns {
		values = append(values, fmt.Sprintf("%s %s", each.Name, each.Type))
	}

	cmd := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", tableName, strings.Join(values, ", "))
	_, err = db.Exec(cmd)
	if err != nil {
		return err
	}
	return nil
}

func RenameColumn(databaseName string, tableName string, oldColumnName string, newColumnName string) error {
	log := CreateLogger("column.RenameColumn")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return err
	}
	defer db.Close()

	cmd := fmt.Sprintf("ALTER TABLE '%s' RENAME COLUMN %#v TO %#v", tableName, oldColumnName, newColumnName)
	_, err = db.Exec(cmd)
	if err != nil {
		return err
	}
	return nil
}

func ChangeColumnType(databaseName string, tableName string, columnName string, newColumnType string) error {
	log := CreateLogger("column.ChangeColumnType")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return err
	}
	defer db.Close()

	cmd := fmt.Sprintf("ALTER TABLE '%s' ALTER COLUMN %#v TYPE %s", tableName, columnName, newColumnType)
	_, err = db.Exec(cmd)
	if err != nil {
		log.WithError(err).Errorf("db.Exec has failed - cmd: %#v", cmd)
		return err
	}
	return nil
}

func ChangeColumnDefault(databaseName string, tableName string, columnName string, newColumnDefault string) error {
	log := CreateLogger("column.ChangeColumnDefault")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return err
	}
	defer db.Close()

	cmd := fmt.Sprintf("ALTER TABLE '%s' ALTER COLUMN %#v SET DEFAULT %#v", tableName, columnName, newColumnDefault)
	_, err = db.Exec(cmd)
	if err != nil {
		log.WithError(err).Errorf("db.Exec has failed - cmd: %#v", cmd)
		return err
	}
	return nil
}

func ChangeColumnNullable(databaseName string, tableName string, columnName string, nullable bool) error {
	log := CreateLogger("column.ChangeColumnNullable")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return err
	}
	defer db.Close()

	cmd := fmt.Sprintf("ALTER TABLE '%s' ALTER COLUMN %#v %s", tableName, columnName, getNullableString(nullable))
	_, err = db.Exec(cmd)
	if err != nil {
		log.WithError(err).Errorf("db.Exec has failed - cmd: %#v", cmd)
		return err
	}
	return nil
}

func getNullableString(nullable bool) string {
	if nullable {
		return "DROP NOT NULL"
	}
	return "SET NOT NULL"
}
