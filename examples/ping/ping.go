/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package main

import (
	"database/sql"
	"flag"
	"fmt"
	go_n1ql "github.com/couchbase/go_n1ql"
	"log"
)

var serverURL = flag.String("server", "http://192.168.1.3:9000",
	"couchbase server URL")

func main() {

	flag.Parse()

	n1ql, err := sql.Open("n1ql", *serverURL)
	if err != nil {
		log.Fatal(err)
		fmt.Println("Error in open")
	}

	err = n1ql.Ping()
	if err != nil {
		fmt.Println("Error in ping")
		log.Fatal(err)

	}

	ac := []byte(`[{"user": "admin:Administrator", "pass": "asdasd"}]`)

	go_n1ql.SetQueryParams("creds", string(ac))
	go_n1ql.SetQueryParams("timeout", "10s")

	rows, err := n1ql.Query("create index idx on `beer-sample`(abv)")

	if err != nil {
		fmt.Println("Error in query. Error", err)
	}

	if err == nil {
		for rows.Next() {
			var row string
			if err := rows.Scan(&row); err != nil {
				log.Fatal(err)
			}
			log.Printf(" Row %v", row)
		}
	}

	rows, err = n1ql.Query("select * from `beer-sample` where abv is not null limit 10")

	if err != nil {
		fmt.Println("Error in query")
		log.Fatal(err)
	}

	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			log.Fatal(err)
		}
		log.Printf(" Row %v", row)
	}

}
