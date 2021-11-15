package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type ImportCmd struct {
	Table       *string
	File        *string
	Delim       *string
	Db          *string
	Concurrency *int
}

func (c *ImportCmd) Flags(fs *pflag.FlagSet) {
	c.Table = fs.String("table", "", "Table")
	c.File = fs.String("file", "", "File")
	c.Delim = fs.String("delim", ",", "CSV Delimiter")
	c.Db = fs.String("db", "", "Db")
	c.Concurrency = fs.Int("concurrency", 1, "Concurrency")
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

	query := ""                                  // query statement
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

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err.Error())
		}

		if isFirstRow {

			c.parseColumns(record, &query)
			isFirstRow = false

		} else if <-available { // wait for available database connection

			connections += 1
			id += 1
			wg.Add(1)
			go c.insert(id, query, db, callback, &connections, &wg, string2interface(record))
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

	*query = "INSERT INTO " + *c.Table + " ("
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

// convert []string to []interface{}
func string2interface(s []string) []interface{} {

	i := make([]interface{}, len(s))
	for k, v := range s {
		i[k] = v
	}
	return i
}
