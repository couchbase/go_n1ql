package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"github.com/couchbase/go-couchbase"
	_ "github.com/couchbase/go_n1ql"
	"log"
	"os"
)

var serverURL = flag.String("server", "http://localhost:9000",
	"couchbase server URL")

func main() {

	flag.Parse()

	c, err := couchbase.Connect(*serverURL)
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}

	pool, err := c.GetPool("default")
	if err != nil {
		log.Fatalf("Error getting pool:  %v", err)
	}

	bucket, err := pool.GetBucket("beer-sample")
	if err != nil {
		log.Fatalf("Error getting bucket:  %v", err)
	}

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

	name := "brewery"
	rows, err := n1ql.Query("select meta(`beer-sample`) from `beer-sample` where type = ?", name)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()
	rowsReturned := 0

	var value interface{}
	var cas uint64

	for rows.Next() {
		var thisRow []byte
		if err := rows.Scan(&thisRow); err != nil {
			log.Fatal(err)
		}

		rowsReturned++
		var beer interface{}
		json.Unmarshal(thisRow, &beer)
		switch b := beer.(type) {
		case map[string]interface{}:
			// extract the key and cas from the returned rows
			meta := b["$1"].(map[string]interface{})
			key := meta["id"].(string)
			casq := meta["cas"].(float64)

			// now get the key from the couchbase server
			err := bucket.Gets(key, &value, &cas)
			if err != nil {
				log.Fatalf(" Error getting key %v. Err %v", key, err)
			}

			if uint64(casq) != cas {
				log.Fatalf("Cas values don't match for key %v from query %v cas value from library %v", key, casq, cas)
			}

			log.Printf("Cas values match for key %v Cas query %v Cas lib %v", key, uint64(casq), cas)

		default:
			log.Fatalf(" Type is %T", b)
		}

	}

	log.Printf("=== success. All cas values match =====")

}
