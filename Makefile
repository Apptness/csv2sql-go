all: csv2sql

linux:
	GOOS=linux GOARCH=amd64 make cvs2mysql

csv2sql:
	CGO_ENABLED=0 go build -gcflags="-e"

clean:
	rm -f csv2sql-go
