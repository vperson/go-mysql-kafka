package blp

import "github.com/siddontang/go-mysql/schema"

// DBSyncPayload the payload struct of database sync
type DBSyncPayload struct {

	// binlog event type: UPDATE/INSERT/DELETE
	EventType string `json:"eventType"`

	// the database name
	Db string	`json:"db"`
	// the table name
	Table string `json:"table"`
	// the table columns: name to column definition
	Columns map[string]schema.TableColumn `json:"columns"`
	// the primary key column name
	PKColumn string `json:"pkColumn"`

	// the binlog file name
	LogFile string `json:"logFile"`
	// the offset position in the binlog file
	LogFileOffset string `json:"logfileOffset"`

	// the timestamp of the binlog event
	Ts uint8 `json:"ts"`

	// the rows batch of binlog event
	Rows []*RowChange `json:"rows"`

	// the columns changed in this batch
	ColumnsChanged []string `json:"columnsChanged"`
}

// RowChange the struct to describe a single row change
type RowChange struct {
	// the complete snapshot of the single row data
	// when INSERT & UPDATE: store the post update row data
	// when DELETE: store the pre update row data
	Snapshot map[string]interface{} `json:"snapshot"`

	// Only used when UPDATE, store the pre update value of columns
	// corresponding to *ColumnsChanged* in *DBSyncPayload*
	PreUpdate map[string]interface{} `json:"preUpdate"`
}
