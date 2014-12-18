//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package go_n1ql

import (
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
)

func TestConnection(t *testing.T) {
	conn, err := OpenN1QLConnection("http://localhost:8093")
	if err != nil {
		t.Fatal(err)
	}

	results, err := conn.(*n1qlConn).Query("select * from `beer-sample`", nil)
	if err != nil {
		t.Fatal(err)
	}

	result := make([]driver.Value, 1)
	totalRows := 0
	for results.Next(result) != io.EOF {
		totalRows++
	}

	if totalRows != 7303 {
		t.Fatal(" Got the wrong number of rows ", totalRows)
	}

	results, err = conn.(*n1qlConn).Query("select * from `gamesim-sample`", nil)
	if err != nil {
		t.Fatal(err)
	}

	totalRows = 0
	for results.Next(result) != io.EOF {
		totalRows++
		if totalRows == 100 {
			results.Close()
		}
	}
	fmt.Printf(" Got %d rows from gamesim-sample", totalRows)

	stmt, err := conn.Prepare("select * from `beer-sample` where `beer-sample`.type = \"beer\" limit 10")
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
	fmt.Printf(" Got %d rows from the last query", totalRows)

	res, err := conn.(*n1qlConn).Exec("upsert into default key \"irish2\" values {\"name\":\"irish\", \"type\":\"contact\"}\"", nil)
	if err != nil {
		t.Fatal(err)
	}

	ra, _ := res.RowsAffected()
	fmt.Printf(" Number of rows inserted %d", ra)

}
