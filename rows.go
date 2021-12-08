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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
)

type n1qlRows struct {
	resp        *http.Response
	results     io.Reader
	resultChan  chan interface{}
	errChan     chan error
	closed      bool
	signature   interface{}
	extras      interface{}
	metrics     interface{}
	errors      interface{}
	passthrough bool
	columns     []string
	rowsSent    int
}

func resultToRows(results io.Reader, resp *http.Response, signature interface{}, metrics, errors, extraVals interface{}) (*n1qlRows, error) {

	rows := &n1qlRows{results: results,
		resp:       resp,
		signature:  signature,
		extras:     extraVals,
		metrics:    metrics,
		errors:     errors,
		resultChan: make(chan interface{}, 1),
		errChan:    make(chan error),
	}

	// detect if we are in passthrough mode
	if metrics != nil && extraVals != nil {
		rows.passthrough = true
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

	// second row will be metrics
	if rows.metrics != nil {
		rows.resultChan <- rows.metrics
	}

	for _, row := range resultRows {
		if rows.closed == true {
			break
		}
		rows.resultChan <- row
	}

	if rows.errors != nil {
		rows.resultChan <- rows.errors
	}

	close(rows.resultChan)

}

func (rows *n1qlRows) Columns() []string {

	var columns = make([]string, 0)

	switch s := rows.signature.(type) {
	case map[string]interface{}:
		for key, _ := range s {
			columns = append(columns, key)
		}
	case string:
		columns = append(columns, s)
	case nil:
		columns = append(columns, "null")
	}

	sort.Strings(columns)
	rows.columns = columns
	return columns
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
			} else if rows.passthrough == true && rows.rowsSent < 2 {
				// first two rows in passthrough mode are status and metrics
				// in passthrough mode if the query being executed has multiple projections
				// then it is highly likely that the number of columns of the metrics/status
				// will not match the number of columns, therefore the following hack
				bytes, _ := json.Marshal(r)
				dest[0] = bytes
				for i := 1; i < numColumns; i++ {
					dest[i] = ""
				}
			} else {
				switch resultRow := r.(type) {
				case map[string]interface{}:
					if len(resultRow) > numColumns {
						return fmt.Errorf("N1QL: More Colums than expected %d != %d r %v", len(resultRow), numColumns, r)
					}
					i := 0
					for _, colName := range rows.columns {
						if value, exists := resultRow[colName]; exists == true {
							bytes, _ := json.Marshal(value)
							dest[i] = bytes

						} else {
							dest[i] = ""
						}
						i++
					}
				case []interface{}:
					i := 0
					for _, value := range resultRow {
						bytes, _ := json.Marshal(value)
						dest[i] = bytes
						i++
					}

				}
			}
			rows.rowsSent++
			return nil
		} else {
			return io.EOF
		}
	case e := <-rows.errChan:
		return e
	}
}
