package sqlengine

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // MySQL Driver

	"github.com/pivotal-golang/lager"
)

type MySQLEngine struct {
	logger lager.Logger
	db     *sql.DB
}

func NewMySQLEngine(logger lager.Logger) *MySQLEngine {
	return &MySQLEngine{
		logger: logger.Session("mysql-engine"),
	}
}

func (d *MySQLEngine) Open(address string, port int64, dbname string, username string, password string) error {
	connectionString := d.connectionString(address, port, dbname, username, password)
	d.logger.Debug("sql-open", lager.Data{"connection-string": connectionString})

	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return err
	}

	d.db = db

	return nil
}

func (d *MySQLEngine) Close() {
	if d.db != nil {
		d.db.Close()
	}
}

func (d *MySQLEngine) ExistsDB(dbname string) (bool, error) {
	selectDatabaseStatement := "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?"
	d.logger.Debug("database-exists", lager.Data{"statement": selectDatabaseStatement, "params": []string{dbname}})

	var dummy string
	err := d.db.QueryRow(selectDatabaseStatement, dbname).Scan(&dummy)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func (d *MySQLEngine) CreateDB(dbname string) error {
	ok, err := d.ExistsDB(dbname)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	createDBStatement := "CREATE DATABASE IF NOT EXISTS ?"
	d.logger.Debug("create-database", lager.Data{"statement": createDBStatement, "params": []string{dbname}})

	if _, err := d.db.Exec(createDBStatement, dbname); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *MySQLEngine) DropDB(dbname string) error {
	dropDBStatement := "DROP DATABASE IF EXISTS ?"
	d.logger.Debug("drop-database", lager.Data{"statement": dropDBStatement, "params": []string{dbname}})

	if _, err := d.db.Exec(dropDBStatement, dbname); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *MySQLEngine) CreateUser(username string, password string) error {
	createUserStatement := "CREATE USER ? IDENTIFIED BY ?"
	d.logger.Debug("create-user", lager.Data{"statement": createUserStatement, "params": []string{username, password}})

	if _, err := d.db.Exec(createUserStatement, username, password); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *MySQLEngine) DropUser(username string) error {
	dropUserStatement := "DROP USER ?@'%'"
	d.logger.Debug("drop-user", lager.Data{"statement": dropUserStatement, "params": []string{username}})

	if _, err := d.db.Exec(dropUserStatement, username); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *MySQLEngine) Privileges() (map[string][]string, error) {
	privileges := make(map[string][]string)

	selectPrivilegesStatement := "SELECT db, user FROM mysql.db"
	d.logger.Debug("database-privileges", lager.Data{"statement": selectPrivilegesStatement})

	rows, err := d.db.Query(selectPrivilegesStatement)
	if err != nil {
		d.logger.Error("sql-error", err)
		return privileges, err
	}
	defer rows.Close()

	var dbname, username string
	for rows.Next() {
		err := rows.Scan(&dbname, &username)
		if err != nil {
			d.logger.Error("sql-error", err)
			return privileges, err
		}
		if _, ok := privileges[dbname]; !ok {
			privileges[dbname] = []string{}
		}
		privileges[dbname] = append(privileges[dbname], username)
	}
	err = rows.Err()
	if err != nil {
		d.logger.Error("sql-error", err)
		return privileges, err
	}

	d.logger.Debug("database-privileges", lager.Data{"output": privileges})

	return privileges, nil
}

func (d *MySQLEngine) GrantPrivileges(dbname string, username string) error {
	grantPrivilegesStatement := "GRANT ALL PRIVILEGES ON ?.* TO ?@'%'"
	d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement, "params": []string{dbname, username}})

	if _, err := d.db.Exec(grantPrivilegesStatement, dbname, username); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *MySQLEngine) RevokePrivileges(dbname string, username string) error {
	revokePrivilegesStatement := "REVOKE ALL PRIVILEGES ON ?.* from ?@'%'"
	d.logger.Debug("revoke-privileges", lager.Data{"statement": revokePrivilegesStatement, "params": []string{dbname, username}})

	if _, err := d.db.Exec(revokePrivilegesStatement, dbname, username); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *MySQLEngine) URI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("mysql://%s:%s@%s:%d/%s?reconnect=true", username, password, address, port, dbname)
}

func (d *MySQLEngine) JDBCURI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("jdbc:mysql://%s:%d/%s?user=%s&password=%s", address, port, dbname, username, password)
}

func (d *MySQLEngine) connectionString(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, address, port, dbname)
}
