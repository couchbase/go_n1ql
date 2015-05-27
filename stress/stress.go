package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
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
var repeat = flag.Int("repeat", 1, "number of times to repeat each query")
var prepared = flag.Bool("prepared", false, "use prepared statements")

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
		go runQuery(*serverURL, queryLines, *repeat)
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

func runQuery(server string, queryLines []string, repeat int) {

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
	os.Setenv("n1ql_measure_latency", "true")

	_, err = n1ql.Exec("Create primary index on `beer-sample`")
	if err != nil {
		//log.Fatal(err)
	}

	for i, query := range queryLines {

		var lastRow string
		var avgTime float64
		var rows *sql.Rows
		for j := 0; j < repeat; j++ {

			if *prepared == true {
				stmt, err := n1ql.Prepare(query)
				if err != nil {
					log.Fatal("Error in preparing statement %v", err)
				}

				rows, err = stmt.Query()

				if err != nil {
					log.Fatal("Error Query Line ", err, query, i)
				}

			} else {
				rows, err = n1ql.Query(query)
			}

			if err != nil {
				log.Fatal("Error Query Line ", err, query, i)
			}

			rowsReturned := 0
			for rows.Next() {
				var contacts string
				if err := rows.Scan(&contacts); err != nil {
					log.Fatal(err)
				}
				lastRow = contacts
				rowsReturned++
			}
			var latency interface{}
			_ = json.Unmarshal([]byte(lastRow), &latency)
			avgTime = avgTime + latency.(map[string]interface{})["latency"].(float64)

			rows.Close()

			//log.Printf("Rows returned %d : \n", rowsReturned)
			if err := rows.Err(); err != nil {
				log.Fatal(err)
			}
		}
		log.Printf("Average time per query %v ms\n", (avgTime / float64(repeat)))

	}
	wg.Done()

}
