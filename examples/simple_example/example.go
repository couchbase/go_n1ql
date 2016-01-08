package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	go_n1ql "github.com/couchbase/go_n1ql"
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

	// Set query parameters
	ac := []byte(`[{"user": "admin:Administrator", "pass": "asdasd"}]`)
	go_n1ql.SetQueryParams("creds", string(ac))
	go_n1ql.SetQueryParams("timeout", "10s")
	go_n1ql.SetQueryParams("scan_consistency", "request_plus")

	rows, err := n1ql.Query("select element (select * from contacts)")

	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var contacts string
		if err := rows.Scan(&contacts); err != nil {
			log.Fatal(err)
		}
		log.Printf("Row returned %s : \n", contacts)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	rows.Close()

	var name string
	rows, err = n1ql.Query("select name,type from contacts")

	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var contacts string
		if err := rows.Scan(&contacts, &name); err != nil {
			log.Fatal(err)
		}
		log.Printf("Row returned %s %s: \n", contacts, name)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	rows.Close()

	// if the following option is set, go_n1ql will return metrics as the last row
	go_n1ql.SetPassthroughMode(true)
	name = "dave"
	rows, err = n1ql.Query("select * from contacts unnest contacts.children where contacts.name = ? and children.age > ?", name, 10)

	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var contacts string
		if err := rows.Scan(&contacts); err != nil {
			log.Fatal(err)
		}
		log.Printf("Row returned %s : \n", contacts)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	rows.Close()

	rows, err = n1ql.Query("select name,type from contacts")

	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var contacts string
		if err := rows.Scan(&contacts, &name); err != nil {
			log.Fatal(err)
		}
		log.Printf("Row returned %s %s: \n", contacts, name)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	rows.Close()

	// explain statement
	rows, err = n1ql.Query("explain select * from contacts")

	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var contacts string
		if err := rows.Scan(&contacts); err != nil {
			log.Fatal(err)
		}
		log.Printf("Row returned %s : \n", contacts)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	rows.Close()

	go_n1ql.SetPassthroughMode(false)

	// prepared statements with positional args

	stmt, err := n1ql.Prepare("select personal_details, shipped_order_history from users_with_orders where doc_type=? and personal_details.age = ?")

	if err != nil {
		log.Fatal(err)
	}

	rows, err = stmt.Query("user_profile", 60)
	if err != nil {
		log.Fatal(err)
	}

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

	rows.Close()

	// Exec examples
	result, err := n1ql.Exec("Upsert INTO contacts values (\"irish\",{\"name\":\"irish\", \"type\":\"contact\"})")
	if err != nil {
		log.Fatal(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Rows affected %d", rowsAffected)

	stmt, err = n1ql.Prepare("Upsert INTO contacts values (?,?)")
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

	stmt.Close()
	result, err = stmt.Exec("test", "this shouldn't work")
	if err == nil {
		log.Fatal("Statement not closed")
	}

	result, err = n1ql.Exec("delete from contacts use keys ? ", "irish")
	if err != nil {
		log.Fatal(err)
	}

	keys := make([]string, 0)
	for i := 0; i < 20; i++ {
		keys = append(keys, fmt.Sprintf("irish%d", i))
	}

	value, _ = json.Marshal(keys)
	result, err = n1ql.Exec("delete from contacts use keys ?", value)
	if err != nil {
		log.Fatal(err)
	}

	go_n1ql.UnsetQueryParams("scan_consistency")
	result, err = n1ql.Exec("delete from contacts use keys ?", value)
	if err != nil {
		log.Fatal(err)
	}

	go_n1ql.UnsetQueryParams("scan_consistency")

	// error expected
	go_n1ql.SetQueryParams("scan_consistency", "rubbish_plus")
	result, err = n1ql.Exec("delete from contacts use keys ?", value)
	if err == nil {
		log.Fatal("Error expected")
	}

}
