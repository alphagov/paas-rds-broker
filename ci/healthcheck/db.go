package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func dbHandler(w http.ResponseWriter, r *http.Request) {
	ssl := r.FormValue("ssl") != "false"
	driver := r.FormValue("driver")

	err := testDBConnection(ssl, driver)
	if err != nil {
		fmt.Println("Error: " + err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJson(w, map[string]interface{}{
		"success": true,
	})
}

func getDSN(ssl bool, driver string) (string, error) {
	dbURL, err := url.Parse(os.Getenv("DATABASE_URL"))
	if err != nil {
		return "", err
	}

	switch driver {
	case "postgres":
		if ssl {
			dbURL.RawQuery = dbURL.RawQuery + "&sslmode=verify-full"
		} else {
			dbURL.RawQuery = dbURL.RawQuery + "&sslmode=disable"
		}
		return dbURL.String(), nil
	case "mysql":
		dbURL.Host = fmt.Sprintf("tcp(%s)", dbURL.Host)
		dbURL.RawQuery = ""
		return strings.Replace(dbURL.String(), "mysql2://", "", -1), nil
	default:
		return "", fmt.Errorf("Unknown driver: %s", driver)
	}
}

func testDBConnection(ssl bool, driver string) error {
	dsn, err := getDSN(ssl, driver)
	if err != nil {
		return err
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE foo(id integer)")
	if err != nil {
		return err
	}
	defer func() {
		db.Exec("DROP TABLE foo")
	}()

	_, err = db.Exec("INSERT INTO foo VALUES(42)")
	if err != nil {
		return err
	}

	var id int
	err = db.QueryRow("SELECT * FROM foo LIMIT 1").Scan(&id)
	if err != nil {
		return err
	}
	if id != 42 {
		return fmt.Errorf("Expected 42, got %d", id)
	}

	return nil
}
