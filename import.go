package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/fasthash/fnv1a"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type ImportCmd struct {
	Table                 *string
	File                  *string
	Delim                 *string
	Db                    *string
	Concurrency           *int
	Bulk                  *int
	SquashConsecutiveDups *bool
	SquashAllDupsPerBulk  *bool
	IgnoreColumns         *string
	IgnoreColumns_        []string
	IgnoreColumnsMap_     map[int]bool
	RemapColumns          *string
	RemapColumns_         map[string]string
	IgnoreErrors          *bool
}

func (c *ImportCmd) Flags(fs *pflag.FlagSet) {
	c.Table = fs.String("table", "", "Table")
	c.File = fs.String("file", "", "File")
	c.Delim = fs.String("delim", ",", "CSV Delimiter")
	c.Db = fs.String("db", "", "Db")
	c.Concurrency = fs.Int("concurrency", 1, "Concurrency")
	c.Bulk = fs.Int("bulk", 1, "Bulk")
	c.SquashConsecutiveDups = fs.Bool("squash-consecutive-dups", false, "squash consecutive dups")
	c.SquashAllDupsPerBulk = fs.Bool("squash-all-dups-per-bulk", false, "squash all dups per bulk")
	c.IgnoreColumns = fs.String("ignore-columns", "", "ignore columns")
	c.RemapColumns = fs.String("remap-columns", "", "remap columns: x=y,i=j")
	c.IgnoreErrors = fs.Bool("ignore-errors", false, "ignore errors")
}

func (c *ImportCmd) ValidateFlags() error {

	if *c.File == "" {
		return errors.New("Please supply a --file")
	}

	if *c.Table == "" {
		return errors.New("Please supply a --table")
	}

	if *c.Db == "" {
		return errors.New("Please supply a --db (user:pass@host/db)")
	}

	if *c.Concurrency < 0 {
		return errors.New("Please supply a valid --concurrency (>0)")
	}

	if *c.Bulk < 0 {
		return errors.New("Please supply a valid --bulk (>0)")
	}

	c.IgnoreColumnsMap_ = make(map[int]bool)
	if *c.IgnoreColumns != "" {
		c.IgnoreColumns_ = strings.Split(*c.IgnoreColumns, ",")
	}

	c.RemapColumns_ = make(map[string]string)
	if *c.RemapColumns != "" {
		mappings := strings.Split(*c.RemapColumns, ",")
		for _, v := range mappings {
			xy := strings.Split(v, "=")
			if len(xy) < 2 {
				return errors.New("Remap Columns = invalid -> syntx is: column_x=column_y,column_a=column_b,...")
			}
			c.RemapColumns_[xy[0]] = xy[1]
		}
	}

	return nil
}

func (c *ImportCmd) Execute(cmd *cobra.Command, args []string) {

	// --------------------------------------------------------------------------
	// prepare buffered file reader
	// --------------------------------------------------------------------------
	file, err := os.Open(*c.File)
	if err != nil {
		log.Fatal(err.Error())
	}
	reader := csv.NewReader(file)
	reader.Comma = rune((*c.Delim)[0]) // set custom comma for reader (default: ',')

	// --------------------------------------------------------------------------
	// database connection setup
	// --------------------------------------------------------------------------

	db, err := sql.Open("mysql", *c.Db)
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	// check database connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	// set max idle connections
	db.SetMaxIdleConns(*c.Concurrency)
	defer db.Close()

	// --------------------------------------------------------------------------
	// read rows and insert into database
	// --------------------------------------------------------------------------

	start := time.Now() // to measure execution time

	callback := make(chan int)                   // callback channel for insert goroutines
	connections := 0                             // number of concurrent connections
	insertions := 0                              // counts how many insertions have finished
	available := make(chan bool, *c.Concurrency) // buffered channel, holds number of available connections
	for i := 0; i < *c.Concurrency; i++ {
		available <- true
	}

	// start status logger
	c.startLogger(&insertions, &connections)

	// start connection controller
	c.startConnectionController(&insertions, &connections, callback, available)

	var wg sync.WaitGroup
	id := 1
	isFirstRow := true
	firstRowColumns := []string{}

	for {
		records := [][]string{}
		for i := 0; i < *c.Bulk; i++ {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			records = append(records, record)
		}

		if err != nil && err != io.EOF {
			log.Fatal(err.Error())
		}

		if len(records) == 0 {
			break
		}

		if isFirstRow {

			query := "" // query statement

			// filter columns
			columns := []string{}
		ColumnLoop:
			for _, v := range records[0] {
				col := v
				if *c.IgnoreColumns != "" {
					for wk, w := range c.IgnoreColumns_ {
						if v == w {
							c.IgnoreColumnsMap_[wk] = true
							continue ColumnLoop
						}
					}
				}
				if *c.RemapColumns != "" {
					if w, ok := c.RemapColumns_[v]; ok {
						col = w
					}
				}
				columns = append(columns, col)
			}

			c.parseColumns(columns, &query)
			isFirstRow = false
			firstRowColumns = columns

		}

		if <-available { // wait for available database connection

			connections += 1
			id += 1
			wg.Add(1)

			bulkQuery, bulkItems := c.parseBulkColumns(firstRowColumns, records)
			go c.insert(id, bulkQuery, db, callback, &connections, &wg, bulkItems)
		}
	}

	wg.Wait()

	elapsed := time.Since(start)
	log.Printf("Status: %d insertions\n", insertions)
	log.Printf("Execution time: %s\n", elapsed)
}

// inserts data into database
func (c *ImportCmd) insert(id int, query string, db *sql.DB, callback chan<- int, conns *int, wg *sync.WaitGroup, args []interface{}) {

	// make a new statement for every insert,
	// this is quite inefficient, but since all inserts are running concurrently,
	// it's still faster than using a single prepared statement and
	// inserting the data sequentielly.
	// we have to close the statement after the routine terminates,
	// so that the connection to the database is released and can be reused
	stmt, err := db.Prepare(query)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(args...)
	if err != nil {
		log.Printf("ID: %d (%d conns), %s\n", id, *conns, err.Error())
	}

	// finished inserting, send id over channel to signalize termination of routine
	callback <- id
	wg.Done()
}

// controls termination of program and number of connections to database
func (c *ImportCmd) startConnectionController(insertions, connections *int, callback <-chan int, available chan<- bool) {

	go func() {
		for {

			<-callback // returns id of terminated routine

			*insertions += 1  // a routine terminated, increment counter
			*connections -= 1 // and unregister its connection

			available <- true // make new connection available
		}
	}()
}

// print status update to console every second
func (c *ImportCmd) startLogger(insertions, connections *int) {

	go func() {
		c := time.Tick(time.Second)
		for {
			<-c
			log.Printf("Status: %d insertions, %d database connections\n", *insertions, *connections)
		}
	}()
}

// parse csv columns, create query statement
func (c *ImportCmd) parseColumns(columns []string, query *string) {

	*query = "INSERT IGNORE INTO " + *c.Table + " ("
	placeholder := "VALUES ("
	for i, c := range columns {
		if i == 0 {
			*query += c
			placeholder += "?"
		} else {
			*query += ", " + c
			placeholder += ", ?"
		}
	}
	placeholder += ")"
	*query += ") " + placeholder
}

// parse csv columns, create query statement
func (c *ImportCmd) parseBulkColumns(columns []string, bulks [][]string) (string, []interface{}) {

	bulkItems := make([]interface{}, 0)
	bulkItemMap := make(map[uint64]bool)
	query := []string{}

	query = append(query, "INSERT IGNORE INTO "+*c.Table+" (")
	for i, column := range columns {
		query = append(query, column)
		if i != (len(columns) - 1) {
			query = append(query, ",")
		}
	}
	query = append(query, ") VALUES ")
	lastEntryKey := uint64(0)
	for i, entry := range bulks {
		// CRITICAL: this protected us from an invalid csv line.. but, with ignore columns, it won't work!
		// if len(entry) != columnsLen {
		//	continue
		// }
		entryKey := uint64(0)
		if *c.SquashAllDupsPerBulk || *c.SquashConsecutiveDups {
			entryKey = fnv1a.HashString64(strings.Join(entry, ""))
		}
		if *c.SquashAllDupsPerBulk {
			if o, ok := bulkItemMap[entryKey]; ok && o {
				// found, skip!
				continue
			} else {
				bulkItemMap[entryKey] = true
			}
		}
		if *c.SquashConsecutiveDups {
			if lastEntryKey == 0 {
				lastEntryKey = entryKey
			} else {
				if entryKey == lastEntryKey {
					// skip!
					continue
				}
			}
		}
		if i != 0 {
			query = append(query, ",")
		}
		query = append(query, "(")
		for j, jv := range entry {
			if *c.IgnoreColumns != "" {
				if _, ok := c.IgnoreColumnsMap_[j]; ok {
					continue
				}
			}
			query = append(query, "?")
			bulkItems = append(bulkItems, jv)
			if j != (len(entry) - 1) {
				query = append(query, ",")
			}
		}
		query = append(query, ")")
	}

	bulkQuery := strings.Join(query, " ")

	return bulkQuery, bulkItems
}

// convert []string to []interface{}
func string2interface(s []string) []interface{} {

	i := make([]interface{}, len(s))
	for k, v := range s {
		i[k] = v
	}
	return i
}

/*
func strings2interface(s [][]string) []interface{} {
	maxLen := len(s) * len(s[0])
	j := make([]interface{}, maxLen)
	i := 0
	for _, jv := range s {
		for _, iv := range jv {
			if i >= maxLen {
				break
			}
			j[i] = iv
			i += 1
		}
	}
	return j
}
*/
