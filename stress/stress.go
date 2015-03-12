package main

import (
	"database/sql"
	//"encoding/json"
	"flag"
	//"fmt"
	"bufio"
	_ "github.com/couchbaselabs/go_n1ql"
	"log"
	"os"
	"runtime"
	"sync"
)

var serverURL = flag.String("server", "localhost:8093",
	"couchbase server URL")
var threads = flag.Int("threads", 10, "number of threads")
var queryFile = flag.String("queryfile", "querylog", "file containing list of select queries")

var wg sync.WaitGroup

func main() {

	flag.Parse()

	// set GO_MAXPROCS to the number of threads
	runtime.GOMAXPROCS(*threads)

	queryLines, err := readLines(*queryFile)
	if err != nil {
		log.Fatal(" Unable to read from file %s, Error %v", *queryFile, err)
	}

	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go runQuery(*serverURL, queryLines)
	}

	wg.Wait()
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func runQuery(server string, queryLines []string) {

	n1ql, err := sql.Open("n1ql", *serverURL)
	if err != nil {
		log.Fatal(err)
	}

	err = n1ql.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// Set query parameters
	os.Setenv("n1ql_timeout", "1000s")
	ac := []byte(`[{"user": "admin:Administrator", "pass": "asdasd"}]`)
	os.Setenv("n1ql_creds", string(ac))

	_, err = n1ql.Exec("Create primary index on `beer-sample`")
	if err != nil {
		//log.Fatal(err)
	}

	for i, query := range queryLines {

		rows, err := n1ql.Query(query)

		if err != nil {
			log.Fatal("Error Query Line ", err, query, i)
		}
		defer rows.Close()
		rowsReturned := 0
		for rows.Next() {
			var contacts string
			if err := rows.Scan(&contacts); err != nil {
				log.Fatal(err)
			}
			rowsReturned++
		}

		log.Printf("Rows returned %d : \n", rowsReturned)
		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}

	}
	wg.Done()

}
