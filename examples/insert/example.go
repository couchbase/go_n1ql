package main

import (
	"database/sql"
	"flag"
	_ "github.com/couchbase/go_n1ql"
	"log"
	"os"
)

var serverURL = flag.String("server", "http://localhost:9000",
	"couchbase server URL")

func main() {

	flag.Parse()
	n1ql, err := sql.Open("n1ql", *serverURL)
	if err != nil {
		log.Fatal(err)
	}

	err = n1ql.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// Set query parameters
	os.Setenv("n1ql_timeout", "10s")
	ac := []byte(`[{"user": "admin:Administrator", "pass": "asdasd"}]`)
	os.Setenv("n1ql_creds", string(ac))

	result, err := n1ql.Exec("delete from default use keys [\"1\", \"2\", \"3\"]")
	if err != nil {
		log.Printf("Error returned %v", err)
	}

	if result != nil {
		ra, err := result.RowsAffected()
		if err == nil {
			log.Printf("Number of keys deleted %v", ra)
		}
	}

	result, err = n1ql.Exec("insert into default values(\"1\", 1)")
	if err != nil {
		log.Fatal(err)
	}

	ra, err := result.RowsAffected()
	if err == nil {
		log.Printf("Number of rows inserted %v", ra)
	}

	result, err = n1ql.Exec("insert into default values(\"1\", 1), values(\"2\", 2)")
	if err != nil {
		log.Fatal(err)
	}

	ra, err = result.RowsAffected()
	if err == nil {
		log.Printf("Number of rows inserted %v", ra)
	}

	if ra != 1 {
		log.Fatalf("Test failed. Only one row should have been inserted")
	}

	result, err = n1ql.Exec("insert into default values(\"1\", 1), values(\"2\", 2)")
	if err != nil {
		log.Fatal(err)
	}

	ra, err = result.RowsAffected()
	if err == nil {
		log.Printf("Number of rows inserted %v", ra)
	}

	if ra != 0 {
		log.Fatalf("Test failed. No keys should have been inserted")
	}

}
