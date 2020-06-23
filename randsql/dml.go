package randsql

import (
	"fmt"
	"math/rand"
	"unsafe"
)

type DMLKind int

const (
	dmlInsert DMLKind = iota
	dmlUpdate
	dmlDelete
	dmlReplace
)

type dmlJobArg unsafe.Pointer

type dmlJobTask struct {
	k            DMLKind
	sql          string
	table        *tableInfo
	assigns      []*columnDescriptor
	whereColumns []*columnDescriptor
	err          error
}

type columnDescriptor struct {
	column *columnInfo
	value  interface{}
}

func (ddlt *columnDescriptor) getValueString() string {
	// make bit data visible
	if ddlt.column.k == KindBit {
		return fmt.Sprintf("b'%v'", ddlt.value)
	} else {
		return fmt.Sprintf("'%v'", ddlt.value)
	}
}

func (ddlt *columnDescriptor) buildConditionSQL() string {
	var sql string
	if ddlt.value == valueNull || ddlt.value == nil {
		sql += fmt.Sprintf("`%s` IS NULL", ddlt.column.name)
	} else {
		switch ddlt.column.k {
		case KindFloat:
			sql += fmt.Sprintf("abs(`%s` - %v) < 0.0000001", ddlt.column.name, ddlt.getValueString())
		case KindDouble:
			sql += fmt.Sprintf("abs(`%s` - %v) < 0.0000000000000001", ddlt.column.name, ddlt.getValueString())
		default:
			sql += fmt.Sprintf("`%s` = %v", ddlt.column.name, ddlt.getValueString())
		}
	}
	return sql
}

func prepareInsert(table *tableInfo, taskCh chan *dmlJobTask) {
	nameList := ""
	valueList := ""
	assigns := []*columnDescriptor{}
	for colIdx, column := range table.columns {
		assign := &columnDescriptor{
			column: column,
			value:  column.randValue(),
		}
		if colIdx > 0 {
			nameList += ", "
			valueList += ", "
		}
		nameList += column.name
		valueList += fmt.Sprintf("%v", assign.getValueString())
		assigns = append(assigns, assign)
	}
	sql := fmt.Sprintf("insert into `%s`.`%s` (%s) values (%v);", table.dbName, table.tblName, nameList, valueList)
	task := &dmlJobTask{
		k:       dmlInsert,
		table:   table,
		sql:     sql,
		assigns: assigns,
	}
	taskCh <- task
}

func prepareInsertIgnore(table *tableInfo, taskCh chan *dmlJobTask) {
	nameList := ""
	for colIdx, column := range table.columns {
		if colIdx > 0 {
			nameList += ", "
		}
		nameList += column.name
	}

	count := rand.Intn(20) + 1

	valueList := "("
	assigns := []*columnDescriptor{}
	for i := 0; i < count; i++ {
		if i > 0 {
			valueList += "), ("
		}
		for colIdx, column := range table.columns {
			assign := &columnDescriptor{
				column: column,
				value:  column.randValue(),
			}
			if colIdx > 0 {
				valueList += ", "
			}
			valueList += fmt.Sprintf("%v", assign.getValueString())
			assigns = append(assigns, assign)
		}
	}
	valueList += ")"

	sql := fmt.Sprintf("insert ignore into `%s`.`%s` (%s) values %v;", table.dbName, table.tblName, nameList, valueList)
	task := &dmlJobTask{
		k:       dmlInsert,
		table:   table,
		sql:     sql,
		assigns: assigns,
	}
	taskCh <- task
}

func doInsertJob(task *dmlJobTask) error {
	table := task.table
	assigns := task.assigns

	// append row
	table.lock.Lock()
	for _, assign := range assigns {
		for _, col := range table.columns {
			if col == assign.column {
				col.rows = append(col.rows, assign.value)
			}
		}
	}
	table.numberOfRows += (len(assigns) / len(table.columns))
	table.lock.Unlock()
	return nil
}

func buildWhereColumns(col *columnInfo) []*columnDescriptor {
	if len(col.rows) == 0 {
		return nil
	}
	// build where conditions
	whereColumns := make([]*columnDescriptor, 0, 1)
	rowToUpdate := rand.Intn(len(col.rows))
	whereColumns = append(whereColumns, &columnDescriptor{column: col, value: col.rows[rowToUpdate]})
	return whereColumns
}

func prepareUpdate(table *tableInfo, taskCh chan *dmlJobTask) {
	if table.numberOfRows == 0 {
		return
	}
	table.lock.Lock()
	randCol := table.columns[rand.Intn(len(table.columns))]
	// build where conditions
	whereColumns := buildWhereColumns(randCol)
	table.lock.Unlock()

	assigns := []*columnDescriptor{}
	for _, column := range table.columns {
		assign := &columnDescriptor{
			column: column,
			value:  column.randValue(),
		}
		assigns = append(assigns, assign)
	}

	// build SQL
	sql := fmt.Sprintf("UPDATE `%s`.`%s` SET ", table.dbName, table.tblName)
	for i, cd := range assigns {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s` = %v", cd.column.name, cd.getValueString())
	}
	if len(whereColumns) > 0 {
		sql += " WHERE "
		for i, cd := range whereColumns {
			if i > 0 {
				sql += " AND "
			}
			sql += cd.buildConditionSQL()
		}
	}

	task := &dmlJobTask{
		k:            dmlUpdate,
		table:        table,
		sql:          sql,
		assigns:      assigns,
		whereColumns: whereColumns,
	}
	taskCh <- task
}

func doUpdateJob(task *dmlJobTask) error {
	table := task.table
	assigns := task.assigns
	whereColumns := task.whereColumns
	// update values
	table.lock.Lock()
	defer table.lock.Unlock()
	for i := 0; i < table.numberOfRows; i++ {
		match := true
		for _, cd := range whereColumns {
			row := cd.column.rows[i]
			if cd.value != row {
				match = false
				break
			}
		}
		if match {
			for _, cd := range assigns {
				cd.column.rows[i] = cd.value
			}
		}
	}
	return nil
}

func prepareDelete(table *tableInfo, taskCh chan *dmlJobTask) {
	table.lock.Lock()

	randCol := table.columns[rand.Intn(len(table.columns))]
	// build where conditions
	whereColumns := buildWhereColumns(randCol)
	table.lock.Unlock()

	sql := fmt.Sprintf("delete from `%s`.`%s`", table.dbName, table.tblName)
	if len(whereColumns) > 0 {
		sql += " where "
		for i, cd := range whereColumns {
			if i > 0 {
				sql += " and "
			}
			sql += cd.buildConditionSQL()
		}
	}

	task := &dmlJobTask{
		k:            dmlDelete,
		table:        table,
		sql:          sql,
		whereColumns: whereColumns,
	}
	taskCh <- task
}

func doDeleteJob(task *dmlJobTask) error {
	table := task.table
	whereColumns := task.whereColumns

	// update values
	table.lock.Lock()
	defer table.lock.Unlock()
	for i := table.numberOfRows - 1; i >= 0; i-- {
		match := true
		for _, cd := range whereColumns {
			row := cd.column.rows[i]
			if cd.value != row {
				match = false
				break
			}
		}
		if match {
			// we must use `table.columns` here, since there might be new columns after deletion
			for _, col := range table.columns {
				tmp := col.rows[i+1:]
				col.rows = col.rows[:i]
				col.rows = append(col.rows, tmp...)
			}
			table.numberOfRows--
		}
	}
	return nil
}

func prepareReplace(table *tableInfo, taskCh chan *dmlJobTask) {
	assigns := []*columnDescriptor{}
	for _, column := range table.columns {
		assign := &columnDescriptor{
			column: column,
			value:  column.randValue(),
		}
		assigns = append(assigns, assign)
	}

	sql := ""
	sql = fmt.Sprintf("replace into `%s`.`%s` set ", table.dbName, table.tblName)
	perm := rand.Perm(len(assigns))
	for i, idx := range perm {
		assign := assigns[idx]
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s` = %v", assign.column.name, assign.getValueString())
	}

	task := &dmlJobTask{
		k:       dmlReplace,
		sql:     sql,
		table:   table,
		assigns: assigns,
	}
	taskCh <- task
}

func doReplaceJob(task *dmlJobTask) error {
	return doInsertJob(task)
}

func doDMLJobInLocal(task *dmlJobTask) error {
	switch task.k {
	case dmlInsert:
		return doInsertJob(task)
	case dmlUpdate:
		return doUpdateJob(task)
	case dmlDelete:
		return doDeleteJob(task)
	case dmlReplace:
		return doReplaceJob(task)
	}
	return fmt.Errorf("unknow dml task , %v", *task)
}
