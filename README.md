# go_n1ql

## N1QL Driver for Go's `database/sql` package

This library implements the standard go database APIs for
[database/sql](http://golang.org/pkg/database/sql/) and 
[database/sql/driver](http://golang.org/pkg/database/sql/driver/).

## Installation

```bash
go get github.com/couchbaselabs/go_n1ql
cd $GOPATH/src/github.com/couchbaselabs/go_n1ql
go get ...
```

## Example Application 

See [./example/example.go](https://github.com/couchbaselabs/go_n1ql/blob/master/example/example.go)

Start
```bash
cbq_engine ./cbq_engine -datastore=dir:../../test/json
./example
```

## Imports 

To use the `go_n1ql` driver the following two imports are required

```go
import (
    "database/sql"
    _ "github.com/couchbaselabs/go_n1ql"
)
```

## Connecting to N1QL

The `go_n1ql` driver allows you to connect to either a standalone instance of N1QL or 
a couchbase cluster endpoint.

### Connect to a standalone N1QL instance

```go
n1ql, err := sql.Open("n1ql", "localhost:8093")
```
### Connect to a couchbase cluster

```go
n1ql, err := sql.Open("n1ql", "http://localhost:9000/")
```
The driver will discover the N1QL endpoints in the cluster and connect to one of them.

## Query Options 

Various Query options are supported by `go_n1ql`, these need to be set by calling `os.Setenv` in 
your application

```go
os.Setenv("n1ql_timeout", "10s")
ac := []byte(`[{"user": "admin:Administrator", "pass": "asdasd"}]`)
os.Setenv("n1ql_creds", string(ac))
```
|Query Option|Definition|
|---|---|
|  `n1ql_timeout` |Set the timeout for a query i.e. `1000ms`, `10s`, `1m`, etc
|  `n1ql_encoding`| Desired character encodings for query results. Currently only UTF-8 is supported
|  `n1ql_compression` |Compression format to use for response data on the wire. Possible values are `ZIP`, `RLE`, `LZMA`, `LZO`, `NONE`. Values are case-insensitive. Default value is `NONE`
|  `n1ql_scan_consistency` |Specify the consistency guarantee/constraint for index scanning.  Possible values are `not_bounded`, `request_plus`, `statement_plus` and `at_plus`. Default: `not_bounded`
|  `n1ql_scan_vector` |(Required if scan_consistency=at_plus) Specify the lower bound vector timestamp when using `at_plus` scan consistency.
|  `n1ql_scan_wait` |Can be supplied with `scan_consistency` values of `request_plus`, `statement_plus` and `at_plus`. Specifies the maximum time the client is willing to wait for an index to catch up to the vector timestamp in the request
|  `n1ql_creds` |A list of credentials, in the form of user/password objects
|  `n1ql_client_context_id` |A piece of data supplied by the client that, if present, is echoed in the response

## Running Select Queries 

### Running queries without positional parameters 

```go
rows, err := n1ql.Query("select * from contacts where contacts.name = \"dave\"")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()
for rows.Next() {
    var contacts string
    if err := rows.Scan(&contacts); err != nil {
        log.Fatal(err)
    }
    log.Printf("Row returned %s : \n", contacts)
}
```

Note that since Couchbase is a document oriented database there are no columns. Each document in the 
database is treated as a row. For queries of the form `SELECT * FROM bucket` the results will be 
returned in a single column. Queries where the result expression is not `*` will return the results in 
multiple columns.

#### Example query returning multiple columns

```go
rows, err := n1ql.Query("select personal_details, shipped_order_history from users_with_orders where doc_type=\"user_profile\" and personal_details.age = 60")

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
```

### Running queries with positional parameters 

Positional parameters are supported by the Queryer/Execer interface and by the Statement (prepared statement) interface

#### Example of a Prepared statement with positional parameters

```go
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
```

## Running DML Queries 

DML queries are supported via the Execer and Statment interface. These statements are not expected to return 
any rows, instead the number of rows mutated/modified will be returned

### Example usage of the Execer interface

```go
result, err := n1ql.Exec("Upsert INTO contacts values (\"irish\",{\"name\":\"irish\", \"type\":\"contact\"})")
if err != nil {
    log.Fatal(err)
}

rowsAffected, err := result.RowsAffected()
if err != nil {
    log.Fatal(err)
}
log.Printf("Rows affected %d", rowsAffected)
```

### Example usage of Prepared Statements with Exec

```go
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
```

### Note
Any positional values that contain either arrays or maps or any combination thereof 
need to be marshalled and passed as type `[]byte`

