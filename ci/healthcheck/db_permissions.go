package main

import (
	"database/sql"
	"fmt"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

const (
	permissionCheckTableName = "permissions_check"
)

func dbPermissionsCheckHandler(w http.ResponseWriter, r *http.Request) {
	var err error

	phase := r.FormValue("phase")
	ssl := r.FormValue("ssl") != "false"
	driver := r.FormValue("driver")

	dsn, err := getDSN(ssl, driver)
	if err != nil {
		fmt.Println("Error: " + err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	switch phase {
	case "setup":
		err = setupPermissionsCheck(driver, dsn)
	case "test":
		err = testPermissionsCheck(driver, dsn)
	default:
		http.Error(w, fmt.Sprintf("Invalid phase '%s' in request.", phase), http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJson(w, map[string]interface{}{
		"success": true,
	})
}

func setupPermissionsCheck(driver, dsn string) error {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE " + permissionCheckTableName + "(id integer)")
	if err != nil {
		return fmt.Errorf("Error creating table: %s", err.Error())
	}
	_, err = db.Exec("INSERT INTO " + permissionCheckTableName + " VALUES(42)")
	if err != nil {
		return fmt.Errorf("Error inserting record: %s", err.Error())
	}

	return nil
}

func testPermissionsCheck(driver, dsn string) error {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	// Can we write?
	_, err = db.Exec("INSERT INTO " + permissionCheckTableName + " VALUES(43)")
	if err != nil {
		return fmt.Errorf("Error inserting record: %s", err.Error())
	}

	// Can we ALTER?
	_, err = db.Exec("ALTER TABLE " + permissionCheckTableName + " ADD COLUMN something INTEGER")
	if err != nil {
		return fmt.Errorf("Error ALTERing table: %s", err.Error())
		return err
	}

	// Can we DROP?
	_, err = db.Exec("DROP TABLE " + permissionCheckTableName)
	if err != nil {
		return fmt.Errorf("Error DROPing table: %s", err.Error())
	}

	return nil
}
