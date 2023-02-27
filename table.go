package postgres

import (
	"fmt"
)

func CreateTable(databaseName string, tableName string, columns []Column) error {
	log := CreateLogger("table.CreateTable")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return err
	}
	defer db.Close()

	// Check if database exists
	exists, err := DoesDatabaseExists(databaseName)
	if err != nil {
		log.WithError(err).Error("DoesDatabaseExists has failed")
		return err
	}
	if !exists {
		err = fmt.Errorf("database does not exist - databaseName: %#v", databaseName)
		log.Fatal(err)
	}

	// Check if table exists
	exists, err = DoesTableExists(databaseName, tableName)
	if err != nil {
		log.WithError(err).Error("DoesTableExists has failed")
		return err
	}
	if exists {
		log.Warnf("Table already exists - databaseName: %#v, tableName: %#v", databaseName, tableName)
		return nil
	}

	values := ""
	for ii := 0; ii < len(columns); ii++ {
		values += fmt.Sprintf("%s %s", columns[ii].Name, columns[ii].Type)
		if ii < len(columns)-1 {
			values += ", "
		}
	}

	// Create Table
	cmd := fmt.Sprintf(`CREATE TABLE %#v (%v)`, tableName, values)
	log.Debugf("cmd: %s", cmd)
	_, err = db.Exec(cmd)
	if err != nil {
		log.WithError(err).Errorf("db.Exec has failed - cmd: %#v", cmd)
		return err
	}

	return nil
}

func DoesTableExists(databaseName string, tableName string) (bool, error) {
	log := CreateLogger("table.DoesTableExists")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return false, err
	}
	defer db.Close()

	// Check if table exists
	cmd := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM information_schema.tables `+
		`WHERE table_name='%s')`, tableName)
	log.Debugf("cmd: %s", cmd)

	var exists bool
	err = db.QueryRow(cmd).Scan(&exists)
	if err != nil {
		log.WithError(err).Errorf("row.Scan has failed - cmd: %#v", cmd)
		return false, err
	}

	return exists, nil
}

func DropTable(databaseName string, tableName string) error {
	log := CreateLogger("table.DropTable")

	db, err := GetDatabase(databaseName)
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return err
	}
	defer db.Close()

	// Drop table foreign keys
	cmd := fmt.Sprintf(`DROP TABLE IF EXISTS %#v CASCADE`, tableName)
	log.Debugf("cmd: %s", cmd)

	_, err = db.Exec(cmd)
	if err != nil {
		log.WithError(err).Errorf("db.Exec has failed - cmd: %#v", cmd)
		return err
	}

	// Drop table
	cmd = fmt.Sprintf(`DROP TABLE IF EXISTS %#v`, tableName)
	log.Debugf("cmd: %s", cmd)

	_, err = db.Exec(cmd)
	if err != nil {
		log.WithError(err).Errorf("db.Exec has failed - cmd: %#v", cmd)
		return err
	}

	log.Infof("Table %s has been dropped", tableName)
	return nil
}
