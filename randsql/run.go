package randsql

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

var (
	dbAddr = ""
	passwd = ""
	dbName = ""
)

func getCli() *sql.DB {
	dbDSN := fmt.Sprintf("root:%s@tcp(%s)/%s", passwd, dbAddr, dbName)
	log.Infof("new connection, %v", dbDSN)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		log.Errorf("can not connect to database. ", err)
		os.Exit(1)
	}
	db.SetMaxOpenConns(1)
	return db
}

func Run(db, tableName, addr, pwd string, concurrency, txnSize int) {
	dbAddr = addr
	passwd = pwd
	dbName = db

	log.Infof("Start run generate random data, concurrent: %v, txn: %v", concurrency, txnSize)
	ts := testSuit{
		db: getCli(),
	}
	err := ts.addTableInfo(dbName, tableName)
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	ctx := context.Background()
	//taskCh := make(chan *dmlJobTask, 1024)
	taskCh := make(chan *dmlJobTask, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			size := rand.Intn(txnSize) + 1
			tasks := []*dmlJobTask{}
			conn, err := newConnection()
			defer conn.Close()
			if err != nil {
				log.Fatal(err)
			}
			execNum := 0
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-taskCh:
					if execNum >= size || !ok {
						execNum = 0
						err = execSQL(ctx, conn, "commit", id)
						if err == nil {
							// do dml in local
							for _, task := range tasks {
								doDMLJobInLocal(task)
							}
						}

						// new connection.
						if rand.Float64() >= 0.999 {
							newConn, err := newConnection()
							if err == nil {
								conn.Close()
								conn = newConn
							}
						}
						tasks = nil
						if !ok {
							return
						}
					}
					if execNum == 0 {
						execSQL(ctx, conn, "begin", id)
					}
					err := execSQL(ctx, conn, task.sql, id)
					if err == nil {
						tasks = append(tasks, task)
					}
					execNum++
				}
			}
		}(i)
	}
	funcs := []func(*tableInfo, chan *dmlJobTask){
		prepareInsert,
		prepareInsertIgnore,
		prepareUpdate,
		prepareDelete,
		prepareReplace,
	}
	for i := 0; i < 1000000; i++ {
		perm := rand.Perm(len(funcs))
		for _, idx := range perm {
			fn := funcs[idx]
			fn(ts.table, taskCh)
		}
	}
	close(taskCh)
	wg.Wait()
}

func execSQL(ctx context.Context, conn *sql.Conn, sql string, id int) error {
	_, err := conn.ExecContext(ctx, sql)
	if err != nil {
		log.Infof("[dml] [worker %d], sql: %v ,error: %v", id, sql, err)
	} else {
		log.Infof("[dml] [worker %d], sql: %v", id, sql)
	}
	return err
}

func newConnection() (conn *sql.Conn, err error) {
	for i := 0; i < 20; i++ {
		conn, err = getCli().Conn(context.Background())
		if err == nil {
			return conn, nil
		}
	}
	return nil, err
}
