package sync_manager

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
	"time"
)

type eventHandler struct {
	s *SyncManager
}

type posSaver struct {
	pos   mysql.Position
	force bool
}

// OnRotate the function to handle binlog position rotation
func (e *eventHandler) OnRotate(roateEvent *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(roateEvent.NextLogName),
		Pos:  uint32(roateEvent.Position),
	}

	e.s.syncCh <- posSaver{pos, true}

	return e.s.Ctx.Err()
}

func (e *eventHandler) OnTableChanged(schema string, table string) error {
	return nil
}

// OnDDL the function to handle DDL event
func (e *eventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	e.s.syncCh <- posSaver{nextPos, true}
	return e.s.Ctx.Err()
}

// OnRow the function to handle row changed
func (e *eventHandler) OnRow(rows *canal.RowsEvent) error {
	var (
		reqs []interface{}
		err  error
		//matchFlag = true
	)

	// 处理分表分库逻辑
	if e.s.rowMapper != nil {
		rows = e.s.rowMapper.Transform(rows)
	}

	switch rows.Action {
	case canal.InsertAction:
		e.makeInsertRequest(canal.InsertAction, rows.Rows)
	case canal.DeleteAction:
		e.makeDeleteRequest(canal.DeleteAction, rows.Rows)
	case canal.UpdateAction:
		e.makeUpdateRequest(rows.Rows)
	default:
		err = errors.Errorf("invalid rows action %s", rows.Action)
	}

	reqs, err = e.s.sink.Parse(rows)
	if err != nil {
		e.s.cancel()
		return fmt.Errorf("make %s  parse binlog err: %v, close sync", rows.Action, err)
	}

	if len(reqs) > 0 {
		e.s.syncCh <- reqs
	}
	return e.s.Ctx.Err()
}

// OnXID the function to handle XID event
func (e *eventHandler) OnXID(nextPos mysql.Position) error {
	e.s.syncCh <- posSaver{nextPos, false}
	return e.s.Ctx.Err()
}

func (e *eventHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (e *eventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (e *eventHandler) String() string {
	return "EventHandler"
}

// 记录insert的行数
func (e *eventHandler) makeInsertRequest(action string, rows [][]interface{}) error {
	return e.makeRequest(action, rows)
}

// 记录delete的行数
func (e *eventHandler) makeDeleteRequest(action string, rows [][]interface{}) error {
	return e.makeRequest(action, rows)
}

// for insert and delete
func (e *eventHandler) makeRequest(action string, rows [][]interface{}) error {
	realRows := int64(len(rows))
	realRowsString := string(realRows)
	switch action {
	case canal.DeleteAction:
		e.s.DeleteNum.Add(realRows)
		dbDeleteNum.WithLabelValues(realRowsString).Inc()
	case canal.InsertAction:
		e.s.InsertNum.Add(realRows)
		dbInsertNum.WithLabelValues(realRowsString).Inc()
	default:
		log.Infof("make request no tasks to be processed: None")
	}
	return nil
}

// 统计binlog更新的行数
func (e *eventHandler) makeUpdateRequest(rows [][]interface{}) error {
	if len(rows)%2 != 0 {
		return errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}
	realRows := int64(len(rows) / 2)
	dbUpdateNum.WithLabelValues(string(realRows)).Inc()
	e.s.UpdateNum.Add(realRows)
	return nil
}

func (s *SyncManager) syncLoop() {
	bulkSize := s.c.SourceDB.BulkSize
	if bulkSize == 0 {
		bulkSize = 128
	}

	// 默认200毫秒刷一次
	interval := s.c.SourceDB.FlushBulkTime
	if interval == 0 {
		interval = 200 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer s.wg.Done()

	lastSaveTime := time.Now()

	reqs := make([]interface{}, 0, 1024)

	var pos mysql.Position

	for {
		needFlush := false
		needSavePos := false

		select {
		case v := <-s.syncCh:
			switch v := v.(type) {
			case posSaver:
				now := time.Now()

				if v.force || now.Sub(lastSaveTime) > 3*time.Second {
					lastSaveTime = now
					needFlush = true
					needSavePos = true
					pos = v.pos
				}
			case []interface{}:
				reqs = append(reqs, v...)
				needFlush = len(reqs) > bulkSize
			}
		case <-ticker.C:
			needFlush = true
		case <-s.Ctx.Done():
			if err := s.master.Save(pos); err != nil {
				log.Errorf("save sync loop position %s err %v, close sync", pos, err)
				return
			}
			log.Errorf("save sync loop position %s successfully, close sync", pos)
			return
		}

		// 投递到kafka
		if needFlush {
			if len(reqs) > 0 {
				// 重试机制在kafka配置中就有
				if err := s.sink.Publish(reqs); err != nil {
					log.Errorf("flush message failed : %+v", err)
				}
			}
			reqs = reqs[0:0]
		}

		// 刷新到文件或者redis
		if needSavePos {
			if err := s.master.Save(pos); err != nil {
				log.Errorf("save sync position %s err %v, close sync", pos, err)
				s.cancel()
				return
			}
		}
	}
}
