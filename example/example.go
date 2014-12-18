package main

import (
	"database/sql"
	_ "github.com/couchbaselabs/go_n1ql"
	"log"
)

func main() {

	n1ql, err := sql.Open("n1ql", "http://localhost:8093")
	if err != nil {
		log.Fatal(err)
	}

	err = n1ql.Ping()
	if err != nil {
		log.Fatal(err)
	}

	name := "Jane"
	_, err = n1ql.Query("Select * from contacts where name = ? and age != ?", name, 5)
	if err != nil {
		log.Fatal(err)
	}

	/*
		rows, err := n1ql.Query("Select * from `beer-sample` limit 5")
		if err != nil {
			log.Fatal(err)
		}

		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var beer string
			if err := rows.Scan(&beer); err != nil {
				log.Fatal(err)
			}
			log.Printf("Row returned %s : \n", beer)
		}

		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}

		// Scan
		var beer string
		id := "Minnesota"
		err = n1ql.QueryRow("select * from `beer-sample` where state=?", id).Scan(&beer)

		switch {
		case err == sql.ErrNoRows:
			log.Printf("No user with that ID.")
		case err != nil:
			log.Fatal(err)
		default:
			log.Printf("Row is is %s\n", beer)
		}
	*/

}
