package main

import (
	"flag"

	"github.com/crazycs520/gen_table_data/randsql"
)

var (
	dbName      = flag.String("db", "test", "database name")
	tableName   = flag.String("table", "t", "table name")
	addr        = flag.String("addr", "127.0.0.1:4000", "database addr")
	passwd      = flag.String("passwd", "", "password")
	concurrency = flag.Int("concurrency", 12, "concurrency")
	txnSize     = flag.Int("txn", 50, "txn size")
)

func main() {
	flag.Parse()
	randsql.Run(*dbName, *tableName, *addr,*passwd,*concurrency, *txnSize )
}

