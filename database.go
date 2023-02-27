package postgres

import (
	"fmt"

	"database/sql"

	_ "github.com/lib/pq"
)

const (
	MaxOpenConns    = 0
	MaxIdleConns    = 0
	ConnMaxLifetime = 0
	ConnMaxIdleTime = 0
)

var (
	host     string = "localhost"
	port     int    = 5432
	user     string = "user"
	password string = "password"
)

func GetDatabase(databaseName string) (*sql.DB, error) {
	log := CreateLogger("database.GetDatabase")

	psqlconn := fmt.Sprintf("host=%s "+
		"port=%d "+
		"user=%s "+
		"password=%s "+
		"dbname=%s "+
		"sslmode=disable",
		host, port, user, password, databaseName)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		log.WithError(err).Error("sql.Open has failed")
		return nil, err
	}
	db.SetMaxOpenConns(MaxOpenConns)
	db.SetMaxIdleConns(MaxIdleConns)
	db.SetConnMaxLifetime(ConnMaxLifetime)
	db.SetConnMaxIdleTime(ConnMaxIdleTime)

	return db, nil
}

func CreateDatabase(databaseName string) error {
	log := CreateLogger("database.CreateDatabase")

	exists, err := DoesDatabaseExists(databaseName)
	if err != nil {
		log.WithError(err).Error("doesDatabaseExists has failed")
		return err
	}
	if exists {
		log.Warning("Database already exists")
		return nil
	}

	// Open database
	db, err := GetDatabase("postgres")
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return err
	}
	defer db.Close()

	// Create database
	cmd := fmt.Sprintf("CREATE DATABASE %s", databaseName)
	_, err = db.Exec(cmd)
	if err != nil {
		log.WithError(err).Errorf("db.Exec has failed - CREATE DATABASE %s", databaseName)
		return err
	}

	return nil
}

func DropDatabase(databaseName string) error {
	log := CreateLogger("database.DropDatabase")

	exists, err := DoesDatabaseExists(databaseName)
	if err != nil {
		log.WithError(err).Error("doesDatabaseExists has failed")
		return err
	}
	if !exists {
		log.Warning("Database does not exist")
		return nil
	}

	// Open database
	db, err := GetDatabase("postgres")
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return err
	}
	defer db.Close()

	// Drop database
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE %s", databaseName))
	if err != nil {
		log.WithError(err).Errorf("db.Exec has failed - DROP DATABASE %s", databaseName)
		return err
	}

	return nil
}

func DoesDatabaseExists(databaseName string) (bool, error) {
	log := CreateLogger("database.doesDatabaseExists")

	// Open database
	db, err := GetDatabase("postgres")
	if err != nil {
		log.WithError(err).Error("GetDatabase has failed")
		return false, err
	}
	defer db.Close()

	// Check if database exists
	cmd := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = '%s')`, databaseName)
	log.Debugf("cmd: %s", cmd)

	var exists bool
	err = db.QueryRow(cmd).Scan(&exists)
	if err != nil {
		log.WithError(err).Error("row.Scan has failed")
		return false, err
	}

	return exists, nil
}
