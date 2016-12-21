package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/cloudfoundry-community/go-cfenv"
)

var creds map[string]interface{}

func main() {
	env, _ := cfenv.Current()
	services, _ := env.Services.WithLabel(os.Getenv("SERVICE_NAME"))
	if len(services) != 1 {
		log.Fatalf("Expected one service instance; got %d", len(services))
	}
	creds = services[0].Credentials

	http.HandleFunc("/mysql", mysqlHandler)
	http.HandleFunc("/postgres", postgresHandler)
	http.ListenAndServe(":"+os.Getenv("PORT"), nil)
}

func mysqlHandler(w http.ResponseWriter, r *http.Request) {
	u, _ := url.Parse(creds["uri"].(string))
	u.Host = fmt.Sprintf("tcp(%s)", u.Host)
	u.RawQuery = ""
	dsn := strings.Replace(u.String(), "mysql://", "", -1)

	db, err := sql.Open(u.Scheme, dsn)
	if err != nil {
		reportError(err, w)
		return
	}

	// Write and read data
	_, err = db.Exec("drop table if exists acceptance")
	if err != nil {
		reportError(err, w)
		return
	}

	_, err = db.Exec("create table acceptance (id integer, value text)")
	if err != nil {
		reportError(err, w)
		return
	}

	_, err = db.Exec("insert into acceptance values (1, 'acceptance')")
	if err != nil {
		reportError(err, w)
		return
	}

	var value string
	row := db.QueryRow("select value from acceptance where id = ?", 1)
	err = row.Scan(&value)
	if err != nil {
		reportError(err, w)
		return
	}

	if value != "acceptance" {
		reportError(fmt.Errorf("incorrect value: %s", value), w)
		return
	}

	_, err = db.Exec("drop table acceptance")
	if err != nil {
		reportError(err, w)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func postgresHandler(w http.ResponseWriter, r *http.Request) {
	u, _ := url.Parse(creds["uri"].(string))
	db, err := sql.Open(u.Scheme, creds["uri"].(string))
	if err != nil {
		reportError(err, w)
		return
	}

	// Write and read data
	_, err = db.Exec("drop table if exists acceptance")
	if err != nil {
		reportError(err, w)
		return
	}

	_, err = db.Exec("create table acceptance (id integer, value text)")
	if err != nil {
		reportError(err, w)
		return
	}

	_, err = db.Exec("insert into acceptance values (1, 'acceptance')")
	if err != nil {
		reportError(err, w)
		return
	}

	var value string
	row := db.QueryRow("select value from acceptance where id = $1", 1)
	err = row.Scan(&value)
	if err != nil {
		reportError(err, w)
		return
	}

	if value != "acceptance" {
		reportError(fmt.Errorf("incorrect value: %s", value), w)
		return
	}

	_, err = db.Exec("drop table acceptance")
	if err != nil {
		reportError(err, w)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func reportError(err error, w http.ResponseWriter) {
	log.Println(err.Error())
	w.WriteHeader(http.StatusInternalServerError)
}
