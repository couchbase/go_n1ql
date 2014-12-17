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
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	buffered "github.com/couchbaselabs/query/server/http"
)

// Common error codes
var (
	ErrNotSupported   = fmt.Errorf("N1QL:Not supported")
	ErrNotImplemented = fmt.Errorf("N1QL: Not implemented")
	ErrUnknownCommand = fmt.Errorf("N1QL: Unknown Command")
	ErrInternalError  = fmt.Errorf("N1QL: Internal Error")
)

// defaults
var (
	N1QL_SERVICE_ENDPOINT  = "/query/service"
	N1QL_DEFAULT_HOST      = "127.0.0.1"
	N1QL_DEFAULT_PORT      = 8093
	N1QL_POOL_SIZE         = 2 ^ 10 // 1 MB
	N1QL_DEFAULT_STATEMENT = "SELECT 1"
)

// implements Driver interface
type n1qlDrv struct{}

func init() {
	sql.Register("n1ql", &n1qlDrv{})
}

func (n *n1qlDrv) Open(name string) (driver.Conn, error) {
	return OpenN1QLConnection(name)
}

// implements driver.Conn interface
type n1qlConn struct {
	clusterAddr string
	queryAPI    string
	buffer      buffered.BufferPool
	client      *http.Client
}

// HTTPClient to use for REST and view operations.
var MaxIdleConnsPerHost = 10
var HTTPTransport = &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
var HTTPClient = &http.Client{Transport: HTTPTransport}

func OpenN1QLConnection(name string) (driver.Conn, error) {

	if _, err := url.Parse(name); err != nil {
		return nil, fmt.Errorf("N1QL: Invalid url %s", name)
	}

	name = strings.TrimSuffix(name, "/")
	queryAPI := name + N1QL_SERVICE_ENDPOINT
	conn := &n1qlConn{client: HTTPClient, buffer: buffered.NewSyncPool(N1QL_POOL_SIZE), queryAPI: queryAPI}

	request, err := prepareRequest(N1QL_DEFAULT_STATEMENT, queryAPI, nil, false)
	if err != nil {
		return nil, err
	}

	resp, err := conn.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("N1QL: Connection failed %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("N1QL: Connection failure %s", bod)
	}

	return conn, nil
}

func (conn *n1qlConn) Prepare(query string) (driver.Stmt, error) {

	request, err := prepareRequest(query, conn.queryAPI, nil, true)
	if err != nil {
		return nil, err
	}

	resp, err := conn.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("N1QL: Failed to execute query %s Error  %v", query, err)
	}

	if resp.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("N1QL: Failed to execute query %s", bod)
	}

	var resultMap map[string]*json.RawMessage
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("N1QL: Failed to read response body from server. Error %v", err)
	}

	if err := json.Unmarshal(body, &resultMap); err != nil {
		return nil, fmt.Errorf("N1QL: Failed to parse response. Error %v", err)
	}

	stmt := &n1qlStmt{conn: conn}

	for name, results := range resultMap {
		switch name {
		case "results":
			var preparedResults []interface{}
			if err := json.Unmarshal(*results, &preparedResults); err != nil {
				return nil, fmt.Errorf("N1QL: Failed to unmarshal results %v", err)
			}
			serialized, _ := json.Marshal(preparedResults[0])
			stmt.prepared = string(serialized)
		case "signature":
			stmt.signature = string(*results)
		}
	}

	if stmt.prepared == "" {
		return nil, ErrInternalError
	}

	return stmt, nil
}

func (conn *n1qlConn) Begin() (driver.Tx, error) {
	return nil, ErrNotSupported
}

func (conn *n1qlConn) Close() error {
	return nil
}

func (conn *n1qlConn) Query(query string, args []driver.Value) (driver.Rows, error) {

	request, err := prepareRequest(query, conn.queryAPI, args, false)
	if err != nil {
		return nil, err
	}

	resp, err := conn.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("N1QL: Failed to execute query %s Error  %v", query, err)
	}

	if resp.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("N1QL: Failed to execute query %s", bod)
	}

	var resultMap map[string]*json.RawMessage
	decoder := json.NewDecoder(resp.Body)

	err = decoder.Decode(&resultMap)
	if err != nil {
		return nil, fmt.Errorf(" N1QL: Failed to decode result %v", err)
	}

	for name, results := range resultMap {
		if name == "results" {

			return resultToRows(bytes.NewReader(*results), resp)
		}
	}

	return nil, err
}

// prepare a http request for the query
//
func prepareRequest(query string, queryAPI string, args []driver.Value, prepare bool) (*http.Request, error) {

	postData := url.Values{}

	if query == "" {
		if len(args) > 0 {
			for _, arg := range args {
				switch arg := arg.(type) {
				case string:
					params := strings.SplitN(arg, ":", 2)
					if params[0] != "" && len(params) == 2 {
						postData.Set(params[0], params[1])
					}
				}
			}
		} else {
			return nil, fmt.Errorf("N1QL: Insufficient number of arguments")
		}
	} else {
		if prepare == true {
			query = "PREPARE " + query
		}
		postData.Set("statement", query)
	}

	request, _ := http.NewRequest("POST", queryAPI, bytes.NewBufferString(postData.Encode()))
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	return request, nil
}

type n1qlStmt struct {
	conn      *n1qlConn
	rows      *n1qlRows
	prepared  string
	signature string
}

func (stmt *n1qlStmt) Close() error {
	return nil
}

func (stmt *n1qlStmt) NumInput() int {
	return -1
}

func (stmt *n1qlStmt) Query(args []driver.Value) (driver.Rows, error) {
	if stmt.prepared == "" {
		return nil, fmt.Errorf("N1QL: Prepared statement not found")
	}
	pArgs := make([]driver.Value, 1)
	prepared := "prepared:" + stmt.prepared
	pArgs[0] = prepared
	args = append(pArgs, args...)

	return stmt.conn.Query("", args)
}

func (stmt *n1qlStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, ErrNotImplemented
}
