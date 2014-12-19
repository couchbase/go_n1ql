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

	name := "dave"
	rows, err := n1ql.Query("select * from contacts unnest contacts.children where contacts.name = ? and children.age > ?", name, 10)
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

	// prepared statements with positional args

	stmt, err := n1ql.Prepare("select personal_details, shipped_order_history from users_with_orders where doc_type=? and personal_details.age = ?")

	rows, err = stmt.Query("user_profile", 60)
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
}
