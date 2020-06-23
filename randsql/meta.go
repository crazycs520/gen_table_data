package randsql

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

type tableInfo struct {
	dbName  string
	tblName string
	columns []*columnInfo

	numberOfRows int
	lock         sync.Mutex
}

type columnInfo struct {
	k    int
	name string

	unsigned bool
	filedTypeM     int //such as:  VARCHAR(10) ,    filedTypeM = 10
	filedTypeD     int //such as:  DECIMAL(10,5) ,  filedTypeD = 5
	filedPrecision int
	defaultValue   interface{}
	isPrimaryKey   bool
	rows           []interface{}

	dependenciedCols []*columnInfo
}

func newColumnInfo(name, tp string) (*columnInfo, error) {
	tpPrefix := tp
	tpSuffix := ""
	unsigned := false
	if idx := strings.Index(tp, "unsigned"); idx > 0 {
		tp = strings.TrimSpace(tp[:idx])
		unsigned = true
	}
	if idx := strings.Index(tp, "("); idx > 0 {
		tpPrefix = tp[:idx]
		tpSuffix = strings.TrimSpace(tp[idx:])
	}

	k, ok := str2ColumnTP[tpPrefix]
	if !ok {
		return nil, fmt.Errorf("unknown column tp: %v of column %v", tp, name)
	}
	col := &columnInfo{
		k:    k,
		name: name,
		unsigned: unsigned,
	}
	if tpSuffix == "" {
		return col, nil
	}
	tpSuffix = strings.Trim(tpSuffix, "(")
	tpSuffix = strings.Trim(tpSuffix, ")")
	nums := strings.Split(tpSuffix, ",")
	if len(nums) == 0 {
		return col, nil
	}
	num, err := strconv.ParseInt(nums[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unknown column tp: %v of column %v", tp, name)
	}
	col.filedTypeM = int(num)
	if len(nums) < 2 {
		return col, nil
	}
	num, err = strconv.ParseInt(nums[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unknown column tp: %v of column %v", tp, name)
	}
	col.filedTypeD = int(num)
	return col, nil
}

var str2ColumnTP = map[string]int{
	"bit":        KindBit,
	"text":       KindTEXT,
	"date":       KindDATE,
	"datetime":   KindDATETIME,
	"decimal":    KindDECIMAL,
	"double":     KindDouble,
	"enum":       KindEnum,
	"float":      KindFloat,
	"mediumint":  KindMEDIUMINT,
	"json":       KindJSON,
	"int":        KindInt32,
	"bigint":     KindBigInt,
	"longtext":   KindLONGTEXT,
	"mediumtext": KindMEDIUMTEXT,
	"set":        KindSet,
	"smallint":   KindSMALLINT,
	"char":       KindChar,
	"time":       KindTIME,
	"timestamp":  KindTIMESTAMP,
	"tinyint":    KindTINYINT,
	"tinytext":   KindTINYTEXT,
	"varchar":    KindVarChar,
	"year":       KindYEAR,
}
// randValue return a rand value of the column
func (col *columnInfo) randValue() interface{} {
	switch col.k {
	case KindTINYINT:
		if col.unsigned {
			return rand.Int31n(1<<8)
		}
		return rand.Int31n(1<<8) - 1<<7
	case KindSMALLINT:
		if col.unsigned {
			return rand.Int31n(1<<16)
		}
		return rand.Int31n(1<<16) - 1<<15
	case KindMEDIUMINT:
		if col.unsigned {
			return rand.Int31n(1<<24)
		}
		return rand.Int31n(1<<24) - 1<<23
	case KindInt32:
		if col.unsigned {
			return rand.Int63n(1<<32)
		}
		return rand.Int63n(1<<32) - 1<<31
	case KindBigInt:
		if rand.Intn(2) == 1 || col.unsigned{
			return rand.Int63()
		}
		return -1 - rand.Int63()
	case KindBit:
		if col.filedTypeM >= 64 {
			return fmt.Sprintf("%b", rand.Uint64())
		} else {
			m := col.filedTypeM
			if col.filedTypeM > 7 { // it is a bug
				m = m - 1
			}
			n := (int64)((1 << (uint)(m)) - 1)
			return fmt.Sprintf("%b", rand.Int63n(n))
		}
	case KindFloat:
		return rand.Float32() + 1
	case KindDouble:
		return rand.Float64() + 1
	case KindDECIMAL:
		if col.unsigned {
			value := RandDecimal(col.filedTypeM, col.filedTypeD)
			if len(value) > 0 && value[0] == '-' {
				return value[1:]
			}
		}
		return RandDecimal(col.filedTypeM, col.filedTypeD)
	case KindChar, KindVarChar, KindBLOB, KindTINYBLOB, KindMEDIUMBLOB, KindLONGBLOB, KindTEXT, KindTINYTEXT, KindMEDIUMTEXT, KindLONGTEXT:
		if col.filedTypeM == 0 {
			return ""
		} else {
			return RandSeq(rand.Intn(col.filedTypeM))
		}
	case KindBool:
		return rand.Intn(2)
	case KindDATE:
		randTime := time.Unix(MinDATETIME.Unix()+rand.Int63n(GapDATETIMEUnix), 0)
		return randTime.Format(TimeFormatForDATE)
	case KindTIME:
		randTime := time.Unix(MinTIMESTAMP.Unix()+rand.Int63n(GapTIMESTAMPUnix), 0)
		return randTime.Format(TimeFormatForTIME)
	case KindDATETIME:
		randTime := randTime(MinDATETIME, GapDATETIMEUnix)
		return randTime.Format(TimeFormat)
	case KindTIMESTAMP:
		randTime := randTime(MinTIMESTAMP, GapTIMESTAMPUnix)
		return randTime.Format(TimeFormat)
	case KindYEAR:
		return rand.Intn(254) + 1901 //1901 ~ 2155
	default:
		return nil
	}
}

func randTime(minTime time.Time, gap int64) time.Time {
	// https://github.com/chronotope/chrono-tz/issues/23
	// see all invalid time: https://timezonedb.com/time-zones/Asia/Shanghai
	var randTime time.Time
	for {
		randTime = time.Unix(minTime.Unix()+rand.Int63n(gap), 0).In(Local)
		if NotAmbiguousTime(randTime) {
			break
		}
	}
	return randTime
}

func RandDecimal(m, d int) string {
	ms := randNum(m - d)
	ds := randNum(d)
	var i int
	for i = range ms {
		if ms[i] != byte('0') {
			break
		}
	}
	ms = ms[i:]
	l := len(ms) + len(ds) + 1
	flag := rand.Intn(2)
	//check for 0.0... avoid -0.0
	zeroFlag := true
	for i := range ms {
		if ms[i] != byte('0') {
			zeroFlag = false
		}
	}
	for i := range ds {
		if ds[i] != byte('0') {
			zeroFlag = false
		}
	}
	if zeroFlag {
		flag = 0
	}
	vs := make([]byte, 0, l+flag)
	if flag == 1 {
		vs = append(vs, '-')
	}
	vs = append(vs, ms...)
	if len(ds) == 0 {
		return string(vs)
	}
	vs = append(vs, '.')
	vs = append(vs, ds...)
	return string(vs)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyz1234567890"

func RandSeq(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

const numberBytes = "0123456789"

func randNum(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = numberBytes[rand.Int63()%int64(len(numberBytes))]
	}
	return b
}


type testSuit struct {
	db    *sql.DB
	table *tableInfo
}

func (ts *testSuit) addTableInfo(db, table string) error {
	db = strings.ToLower(db)
	table = strings.ToLower(table)
	query := fmt.Sprintf(`select column_name,column_type from information_schema.columns where lower(TABLE_SCHEMA)='%v' and lower(TABLE_NAME)='%v';`, db, table)
	rows, err := queryResult(ts.db, query)
	if err != nil || len(rows) == 0 {
		return err
	}
	tbInfo := &tableInfo{
		dbName:  db,
		tblName: table,
	}
	for _, row := range rows {
		col, err := newColumnInfo(row[0], row[1])
		if err != nil {
			return err
		}
		tbInfo.columns = append(tbInfo.columns, col)
	}
	ts.table = tbInfo
	return nil
}
