package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
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
		var personal, shipped string
		if err := rows.Scan(&personal, &shipped); err != nil {
			log.Fatal(err)
		}
		log.Printf("Row returned personal_details: %s shipped_order_history %s : \n", personal, shipped)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	// Exec examples
	result, err := n1ql.Exec("Upsert INTO contacts KEY \"irish3\" VALUES {\"name\":\"irish\", \"type\":\"contact\"}")
	if err != nil {
		log.Fatal(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Rows affected %d", rowsAffected)

	stmt, err = n1ql.Prepare("Upsert INTO contacts KEY ?  VALUES ?")
	if err != nil {
		log.Fatal(err)
	}

	// Map Values need to be marshaled
	value, _ := json.Marshal(map[string]interface{}{"name": "irish", "type": "contact"})
	result, err = stmt.Exec("irish4", value)
	if err != nil {
		log.Fatal(err)
	}

	rowsAffected, err = result.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Rows affected %d", rowsAffected)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("irish%d", i)
		result, err = stmt.Exec(key, value)
		if err != nil {
			log.Fatal(err)
		}

		ra, err := result.RowsAffected()
		if err != nil {
			log.Fatal(err)
		}
		rowsAffected += ra
	}

	log.Printf("Total Rows Affected %d", rowsAffected)

}
