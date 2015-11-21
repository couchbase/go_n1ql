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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type n1qlRows struct {
	resp       *http.Response
	results    io.Reader
	resultChan chan interface{}
	errChan    chan error
	closed     bool
	rows       []string
	extras     interface{}
	metrics    interface{}
}

func resultToRows(results io.Reader, resp *http.Response, signature []string, metrics, extraVals interface{}) (*n1qlRows, error) {

	rows := &n1qlRows{results: results,
		resp:       resp,
		rows:       signature,
		extras:     extraVals,
		metrics:    metrics,
		resultChan: make(chan interface{}, 1),
		errChan:    make(chan error),
	}
	go rows.populateRows()

	return rows, nil
}

func (rows *n1qlRows) populateRows() {
	var resultRows []interface{}
	defer rows.resp.Body.Close()

	resultsDecoder := json.NewDecoder(rows.results)
	err := resultsDecoder.Decode(&resultRows)

	if err != nil {
		rows.errChan <- err
	}

	if rows.extras != nil {
		rows.resultChan <- rows.extras
	}

	for _, row := range resultRows {
		if rows.closed == true {
			break
		}
		rows.resultChan <- row
	}

	// last row will be metrics
	if rows.metrics != nil {
		rows.resultChan <- rows.metrics
	}

	close(rows.resultChan)

}

func (rows *n1qlRows) Columns() []string {
	return rows.rows
}

func (rows *n1qlRows) Close() error {
	rows.closed = true
	return nil
}

func (rows *n1qlRows) Next(dest []driver.Value) error {
	select {
	case r, ok := <-rows.resultChan:
		if ok {
			numColumns := len(rows.Columns())

			if numColumns == 1 {
				bytes, _ := json.Marshal(r)
				dest[0] = bytes
			} else {
				switch resultRow := r.(type) {
				case map[string]interface{}:
					if len(resultRow) != numColumns {
						return fmt.Errorf("N1QL: Columns do not match %d != %d r %v", len(resultRow), numColumns, r)
					}
					i := 0
					for _, value := range resultRow {
						bytes, _ := json.Marshal(value)
						dest[i] = bytes
						i++
					}
				}
			}
			return nil
		} else {
			return io.EOF
		}
	case e := <-rows.errChan:
		return e
	}
}
