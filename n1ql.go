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
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/couchbaselabs/go-couchbase"
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
	client      *http.Client
}

// HTTPClient to use for REST and view operations.
var MaxIdleConnsPerHost = 10
var HTTPTransport = &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
var HTTPClient = &http.Client{Transport: HTTPTransport}

func discoverN1QLService(name string, ps couchbase.PoolServices) string {

	for _, ns := range ps.NodesExt {
		if ns.Services != nil {
			if port, ok := ns.Services["n1ql"]; ok == true {
				var hostname string
				//n1ql service found
				if ns.ThisNode == true {
					hostUrl, _ := url.Parse(name)
					hostname = hostUrl.Host
				}
				hostname = ns.Hostname
				host := strings.Split(hostname, ":")
				return fmt.Sprintf("%s:%d", host[0], port)
			}
		}
	}
	return ""
}

func getQueryApi(n1qlEndPoint string) (string, error) {

	queryAdmin := "http://" + n1qlEndPoint + "/admin/clusters/default/nodes"
	request, _ := http.NewRequest("GET", queryAdmin, nil)
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := HTTPClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("N1QL: Failed to execute query Error  %v", err)
	}

	if resp.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("N1QL: Failed to execute query %s", bod)
	}

	var nodesInfo []interface{}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("N1QL: Failed to read response body from server. Error %v", err)
	}

	if err := json.Unmarshal(body, &nodesInfo); err != nil {
		return "", fmt.Errorf("N1QL: Failed to parse response. Error %v", err)
	}

	numQueryNodes := len(nodesInfo)
	sn := 0
	if numQueryNodes > 1 {
		// randomly select one of the nodes
	}
	selectedNode := nodesInfo[sn]

	switch selectedNode := selectedNode.(type) {
	case map[string]interface{}:
		return selectedNode["queryEndpoint"].(string), nil
	}

	return "", fmt.Errorf("Query endpoint not found")
}

func OpenN1QLConnection(name string) (driver.Conn, error) {

	var queryAPI string

	if strings.HasPrefix(name, "http://") {
		// cluster endpoint
		client, err := couchbase.Connect(name)
		if err != nil {
			return nil, fmt.Errorf("N1QL: Unable to connect to cluster endpoint. Error %v", err)
		}

		ps, err := client.GetPoolServices("default")
		if err != nil {
			return nil, fmt.Errorf("N1QL: Failed to get NodeServices list. Error %v", err)
		}

		n1qlEndPoint := discoverN1QLService(name, ps)
		if n1qlEndPoint == "" {
			return nil, fmt.Errorf("N1QL: No query service found on this cluster")
		}

		queryAPI, err = getQueryApi(n1qlEndPoint)
		if err != nil {
			return nil, err
		}

	} else {
		name = strings.TrimSuffix(name, "/")
		queryAPI = "http://" + name + N1QL_SERVICE_ENDPOINT
	}

	conn := &n1qlConn{client: HTTPClient, queryAPI: queryAPI}

	request, err := prepareRequest(N1QL_DEFAULT_STATEMENT, queryAPI, nil)
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
	var argCount int

	query = "PREPARE " + query
	query, argCount = prepareQuery(query)

	request, err := prepareRequest(query, conn.queryAPI, nil)
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

	stmt := &n1qlStmt{conn: conn, argCount: argCount}

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

func decodeSignature(signature *json.RawMessage) []string {

	var sign map[string]interface{}
	rows := make([]string, 0)
	json.Unmarshal(*signature, &sign)

	for row, _ := range sign {
		rows = append(rows, row)
	}

	return rows
}

func (conn *n1qlConn) performQuery(request *http.Request) (driver.Rows, error) {

	resp, err := conn.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("N1QL: Failed to execute query Error  %v", err)
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

	var signature []string
	var resultRows *json.RawMessage
	var haveEnough = 0

	for name, results := range resultMap {
		switch name {
		case "signature":
			signature = decodeSignature(results)
			haveEnough++
		case "results":
			resultRows = results
			haveEnough++
		}
		if haveEnough == 2 {
			break
		}
	}

	return resultToRows(bytes.NewReader(*resultRows), resp, signature)

}

// Executes a query that returns a set of Rows.
// Select statements should use this interface
func (conn *n1qlConn) Query(query string, args []driver.Value) (driver.Rows, error) {

	if len(args) > 0 {
		var argCount int
		query, argCount = prepareQuery(query)
		if argCount != len(args) {
			return nil, fmt.Errorf("Argument count mismatch %d != %d", argCount, len(args))
		}
		query, args = preparePositionalArgs(query, argCount, args)
	}

	request, err := prepareRequest(query, conn.queryAPI, args)
	if err != nil {
		return nil, err
	}

	return conn.performQuery(request)
}

func (conn *n1qlConn) performExec(request *http.Request) (driver.Result, error) {
	resp, err := conn.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("N1QL: Failed to execute query Error  %v", err)
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

	res := &n1qlResult{}
	for name, results := range resultMap {
		switch name {
		case "metrics":
			var metrics map[string]interface{}
			err := json.Unmarshal(*results, &metrics)
			if err != nil {
				return nil, fmt.Errorf("N1QL: Failed to unmarshal response. Error %v", err)
			}
			if mc, ok := metrics["mutationCount"]; ok {
				res.affectedRows = int64(mc.(float64))
			}
			break
		}
	}

	return res, nil

}

// Execer implementation. To be used for queries that do not return any rows
// such as Create Index, Insert, Upset, Delete etc
func (conn *n1qlConn) Exec(query string, args []driver.Value) (driver.Result, error) {

	if len(args) > 0 {
		var argCount int
		query, argCount = prepareQuery(query)
		if argCount != len(args) {
			return nil, fmt.Errorf("Argument count mismatch %d != %d", argCount, len(args))
		}
		query, args = preparePositionalArgs(query, argCount, args)
	}

	request, err := prepareRequest(query, conn.queryAPI, args)
	if err != nil {
		return nil, err
	}

	return conn.performExec(request)
}

func prepareQuery(query string) (string, int) {

	var count int
	re := regexp.MustCompile("\\?")

	f := func(s string) string {
		count++
		return fmt.Sprintf("$%d", count)
	}
	return re.ReplaceAllStringFunc(query, f), count
}

//
// Replace the conditional pqrams in the query and return the list of left-over args
func preparePositionalArgs(query string, argCount int, args []driver.Value) (string, []driver.Value) {
	subList := make([]string, 0)
	newArgs := make([]driver.Value, 0)

	for i, arg := range args {
		if i < argCount {
			var a string
			switch arg := arg.(type) {
			case string:
				a = fmt.Sprintf("\"%v\"", arg)
			case []byte:
				a = string(arg)
			default:
				a = fmt.Sprintf("%v", arg)
			}
			sub := []string{fmt.Sprintf("$%d", i+1), a}
			subList = append(subList, sub...)
		} else {
			newArgs = append(newArgs, arg)
		}
	}
	r := strings.NewReplacer(subList...)
	return r.Replace(query), newArgs
}

// prepare a http request for the query
//
func prepareRequest(query string, queryAPI string, args []driver.Value) (*http.Request, error) {

	postData := url.Values{}
	postData.Set("statement", query)

	if len(args) > 0 {
		paStr := buildPositionalArgList(args)
		if len(paStr) > 0 {
			postData.Set("args", paStr)
		}
	}

	setQueryParams(&postData)
	request, _ := http.NewRequest("POST", queryAPI, bytes.NewBufferString(postData.Encode()))
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	return request, nil
}

//
// Set query params

func setQueryParams(v *url.Values) {

	if timeout := os.Getenv("n1ql_timeout"); timeout != "" {
		v.Set("timeout", timeout)
	}

	if encoding := os.Getenv("n1ql_encoding"); encoding != "" {
		v.Set("encoding", encoding)
	}

	if compression := os.Getenv("n1ql_compression"); compression != "" {
		v.Set("compression", compression)
	}

	if scan_consistency := os.Getenv("n1ql_scan_consistency"); scan_consistency != "" {
		v.Set("scan_consistency", scan_consistency)
	}

	if scan_vector := os.Getenv("n1ql_scan_vector"); scan_vector != "" {
		v.Set("scan_vector", scan_vector)
	}

	if scan_wait := os.Getenv("n1ql_scan_wait"); scan_wait != "" {
		v.Set("scan_wait", scan_wait)
	}

	if creds := os.Getenv("n1ql_creds"); creds != "" {
		v.Set("creds", creds)
	}

	if client_context_id := os.Getenv("n1ql_client_context_id"); client_context_id != "" {
		v.Set("client_context_id", client_context_id)
	}

}

type n1qlStmt struct {
	conn      *n1qlConn
	prepared  string
	signature string
	argCount  int
}

func (stmt *n1qlStmt) Close() error {
	stmt.prepared = ""
	stmt.signature = ""
	stmt.argCount = 0
	stmt = nil
	return nil
}

func (stmt *n1qlStmt) NumInput() int {
	return stmt.argCount
}

func buildPositionalArgList(args []driver.Value) string {
	positionalArgs := make([]string, 0)
	for _, arg := range args {
		switch arg := arg.(type) {
		case string:
			// add double quotes since this is a string
			positionalArgs = append(positionalArgs, fmt.Sprintf("\"%v\"", arg))
		case []byte:
			positionalArgs = append(positionalArgs, string(arg))
		default:
			positionalArgs = append(positionalArgs, fmt.Sprintf("%v", arg))
		}
	}

	if len(positionalArgs) > 0 {
		paStr := "["
		for i, param := range positionalArgs {
			if i == len(positionalArgs)-1 {
				paStr = fmt.Sprintf("%s%s]", paStr, param)
			} else {
				paStr = fmt.Sprintf("%s%s,", paStr, param)
			}
		}
		return paStr
	}
	return ""
}

// prepare a http request for the query
//
func (stmt *n1qlStmt) prepareRequest(args []driver.Value) (*http.Request, error) {

	postData := url.Values{}
	postData.Set("prepared", stmt.prepared)

	if len(args) < stmt.NumInput() {
		return nil, fmt.Errorf("N1QL: Insufficient args. Prepared statement contains positional args")
	}

	if len(args) > 0 {
		paStr := buildPositionalArgList(args)
		if len(paStr) > 0 {
			postData.Set("args", paStr)
		}
	}

	setQueryParams(&postData)

	request, _ := http.NewRequest("POST", stmt.conn.queryAPI, bytes.NewBufferString(postData.Encode()))
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	return request, nil
}

func (stmt *n1qlStmt) Query(args []driver.Value) (driver.Rows, error) {
	if stmt.prepared == "" {
		return nil, fmt.Errorf("N1QL: Prepared statement not found")
	}

	request, err := stmt.prepareRequest(args)
	if err != nil {
		return nil, err
	}

	return stmt.conn.performQuery(request)
}

func (stmt *n1qlStmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.prepared == "" {
		return nil, fmt.Errorf("N1QL: Prepared statement not found")
	}
	request, err := stmt.prepareRequest(args)
	if err != nil {
		return nil, err
	}

	return stmt.conn.performExec(request)
}
