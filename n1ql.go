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

	queryAPI := name + N1QL_SERVICE_ENDPOINT
	conn := &n1qlConn{client: HTTPClient, buffer: buffered.NewSyncPool(N1QL_POOL_SIZE), queryAPI: queryAPI}
	name = strings.TrimSuffix(name, "/")
	postData := url.Values{}
	postData.Set("statement", N1QL_DEFAULT_STATEMENT)

	request, err := http.NewRequest("POST", queryAPI, bytes.NewBufferString(postData.Encode()))
	if err != nil {
		return nil, fmt.Errorf("N1QL: Internal Error %v", err)
	}

	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := conn.client.Do(request)
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("N1QL: Connection failure %s", bod)
	}

	return conn, nil
}

func (conn *n1qlConn) Prepare(query string) (driver.Stmt, error) {
	return nil, ErrNotImplemented
}

func (conn *n1qlConn) Begin() (driver.Tx, error) {
	return nil, ErrNotSupported
}

func (conn *n1qlConn) Close() error {
	return nil
}

// prepare a http request for the query
//
func (conn *n1qlConn) prepareRequest(query string, args []driver.Value) *http.Request {

	postData := url.Values{}
	postData.Set("statement", query)

	// parse the args to generate options for the query
	if len(args) != 0 {
		for _, arg := range args {
			params := strings.SplitN(arg.(string), ":", 1)
			if params[0] != "" && len(params) == 2 {
				postData.Set(params[0], params[1])
			}
		}
	}

	request, _ := http.NewRequest("POST", conn.queryAPI, bytes.NewBufferString(postData.Encode()))
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	return request
}

func (conn *n1qlConn) Query(query string, args []driver.Value) (driver.Rows, error) {

	request := conn.prepareRequest(query, args)

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
		fmt.Printf(" Failed to decode %v", err)
	}

	for name, results := range resultMap {
		if name == "results" {

			return resultToRows(bytes.NewReader(*results), resp)
		}
	}

	return nil, err
}
