package main

import (
	"database/sql"
	"flag"
	go_n1ql "github.com/couchbase/go_n1ql"
	"log"
)

var serverURL = flag.String("server", "http://localhost:8093",
	"couchbase server URL")

func main() {

	flag.Parse()

	go_n1ql.SetPassthroughMode(true)

	n1ql, err := sql.Open("n1ql", *serverURL)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := n1ql.Query("SELECT name, type, hobbies  FROM contacts")
	if err != nil {
		log.Fatal("Error %v", err)
	}

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {

		for i, _ := range columns {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)
		log.Printf("======= Printing Row ==========")
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			log.Println(col, v)
		}
	}
}
