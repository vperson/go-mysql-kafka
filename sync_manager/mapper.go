package sync_manager

import "github.com/siddontang/go-mysql/canal"

type RowMapper interface {
	Transform(e *canal.RowsEvent) *canal.RowsEvent
}

// 默认示例
type DefaultRowMapper struct {
}

// 这块是为了给分表分库预留的, 不做任何处理
func (m *DefaultRowMapper) Transform(e *canal.RowsEvent) *canal.RowsEvent {
	return e
}
