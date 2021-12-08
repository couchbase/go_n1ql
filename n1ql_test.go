//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package go_n1ql

import (
	"database/sql/driver"
	"io"
	"testing"
)

// This test assumes a Couchbase instance is accessible on the local machine,
// and that the standard bucket "beer-sample" is available on it.

func TestConnection(t *testing.T) {
	conn, err := OpenN1QLConnection("http://localhost:8093")
	if err != nil {
		t.Fatal(err)
	}

	results, err := conn.(*n1qlConn).Query("select * from `beer-sample` where city = 'San Francisco'", nil)
	if err != nil {
		t.Fatal(err)
	}

	result := make([]driver.Value, 1)
	totalRows := 0
	for results.Next(result) != io.EOF {
		totalRows++
	}

	if totalRows == 0 {
		t.Fatal("Query returned 0 rows")
	}

	results, err = conn.(*n1qlConn).Query("select * from `beer-sample` where city = \"London\"", nil)
	if err != nil {
		t.Fatal(err)
	}

	totalRows = 0
	for results.Next(result) != io.EOF {
		totalRows++
	}
	results.Close()

	if totalRows != 4 {
		t.Fatal("Expecting 4 rows got %d", totalRows)
	}

	stmt, err := conn.Prepare("select * from `beer-sample` where type = \"brewery\" limit 5")
	if err != nil {
		t.Fatal(err)
	}

	results, err = stmt.Query(nil)
	if err != nil {
		t.Fatal(err)
	}

	totalRows = 0
	for results.Next(result) != io.EOF {
		totalRows++
	}

	if totalRows != 5 {
		t.Fatal(" Got %d Rows instead of 5", totalRows)
	}

	res, err := conn.(*n1qlConn).Exec("upsert into `beer-sample` (key,value) values ('irish2', {'name': 'irish', 'type':'contact'})", nil)
	if err != nil {
		t.Fatal(err)
	}

	ra, _ := res.RowsAffected()
	if ra != 1 {
		t.Fatal("Insert failed.")
	}

	res, err = conn.(*n1qlConn).Exec("delete from `beer-sample` use keys \"irish2\"", nil)
	if err != nil {
		t.Fatal(err)
	}

}
