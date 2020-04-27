package blp

import (
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/schema"
	"reflect"
	"strings"
)

// DATA_FORMAT the data format of timestamp
//const DATE_FORMAT = "2006-01-02T15:04:05.000+08:00"

func parseRowMap(columns *[]schema.TableColumn, row []interface{}) *map[string]interface{} {
	rowMap := make(map[string]interface{})

	nCol := len(*columns)
	if len(row) < nCol {
		nCol = len(row)
	}

	for colId := 0; colId < nCol; colId++ {
		if row[colId] != nil && ((*columns)[colId].RawType == "json" || (*columns)[colId].RawType == "text") {
			rowMap[(*columns)[colId].Name] = string(row[colId].([]uint8))
		} else {
			rowMap[(*columns)[colId].Name] = row[colId]
		}
	}
	return &rowMap
}

func parseColumns(columns *[]schema.TableColumn) *map[string]schema.TableColumn {
	metaMap := make(map[string]schema.TableColumn)

	nCol := len(*columns)

	for colId := 0; colId < nCol; colId++ {
		metaMap[(*columns)[colId].Name] = (*columns)[colId]
	}
	return &metaMap
}

func ParsePayload(e *canal.RowsEvent) *DBSyncPayload {
	var columnChanged []string
	var rowChanges []*RowChange
	if e.Action == canal.InsertAction {
		for _, row := range e.Rows {
			rowChanges = append(rowChanges, &RowChange{
				PreUpdate: map[string]interface{}{},
				Snapshot:  *parseRowMap(&e.Table.Columns, row),
			})
		}
	} else if e.Action == canal.DeleteAction {
		for _, row := range e.Rows {
			rowChanges = append(rowChanges, &RowChange{
				PreUpdate: *parseRowMap(&e.Table.Columns, row),
				Snapshot:  map[string]interface{}{},
			})
		}
	} else if e.Action == canal.UpdateAction {
		for i := 0; i < len(e.Rows); i += 2 {
			pre := e.Rows[i]
			post := e.Rows[i+1]

			beforeUpdate := *parseRowMap(&e.Table.Columns, pre)
			afterUpdate := *parseRowMap(&e.Table.Columns, post)

			if len(columnChanged) == 0 {
				for col := range afterUpdate {
					if afterUpdate[col] == nil || reflect.TypeOf(afterUpdate[col]).Comparable() {
						if afterUpdate[col] != beforeUpdate[col] {
							columnChanged = append(columnChanged, col)
						}
					} else {
						if !reflect.DeepEqual(afterUpdate[col], beforeUpdate[col]) {
							columnChanged = append(columnChanged, col)
						}
					}
				}
			}

			preUpdate := make(map[string]interface{})
			for _, c := range columnChanged {
				preUpdate[c] = beforeUpdate[c]
			}

			rowChanges = append(rowChanges, &RowChange{
				PreUpdate: preUpdate,
				Snapshot:  afterUpdate,
			})
		}
	}

	payload := &DBSyncPayload{
		EventType: strings.ToUpper(e.Action),
		Db:        e.Table.Schema,
		Table:     e.Table.Name,
		//TODO temporally remove scheme information from payload
		//PKColumn: e.Table.GetPKColumn(0).Name,
		//Columns: *parseColumns(&e.Table.Columns),
		Rows:           rowChanges,
		ColumnsChanged: columnChanged,
	}
	return payload
}
