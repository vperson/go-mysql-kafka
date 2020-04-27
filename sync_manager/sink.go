package sync_manager

import "github.com/siddontang/go-mysql/canal"

type Sink interface {
	// 分析binlog接口
	Parse(e *canal.RowsEvent) ([]interface{}, error)
	// 将结果推送到目标
	Publish([]interface{}) error
}
