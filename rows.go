//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package n1ql

import (
	"database/sql/driver"
	"fmt"
	"io"
)

type n1qlRows struct {
	rc         int
	resultRows []interface{}
	cursor     int
}

func resultToRows(result interface{}) (*n1qlRows, error) {

	switch result := result.(type) {
	case map[string]interface{}:
		nr := &n1qlRows{}
		if result["results"] != nil {
			switch resultRows := result["results"].(type) {
			case []interface{}:
				nr.resultRows = resultRows
				nr.rc = len(resultRows)
				return nr, nil
			default:
				fmt.Printf("This is the type %T", result["results"])

			}
		}
	default:
		return nil, fmt.Errorf("N1QL: Failed to decode result")
	}

	return nil, fmt.Errorf("N1QL: Failed to decode result")
}

func (rows *n1qlRows) Columns() []string {
	if rows.rc != 0 {
		return []string{"results"}
	}
	return nil
}

func (rows *n1qlRows) Close() error {
	return nil
}

func (rows *n1qlRows) Next(dest []driver.Value) error {
	if rows.cursor == rows.rc {
		return io.EOF
	}

	nextResult := rows.resultRows[rows.cursor]
	dest[0] = nextResult
	rows.cursor++
	return nil
}
