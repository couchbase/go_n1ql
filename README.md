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

Various Query options can be set by calling SetQueryParams. See example below

```go
import  go_n1ql "github.com/couchbaselabs/go_n1ql"

ac := []byte(`[{"user": "admin:Administrator", "pass": "asdasd"}]`)
go_n1ql.SetQueryParams("creds", ac)
go_n1ql.SetQueryParams("timeout", "10s")
```

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

