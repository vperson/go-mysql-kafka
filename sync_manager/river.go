package sync_manager

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go/sync2"
	log "github.com/sirupsen/logrus"
	"go-mysql-kafka/conf"
	"net/http"
	"sync"
)

type SyncManager struct {
	c *conf.ConfigSet

	canal *canal.Canal

	Ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	master *masterInfo

	// 推送内容到目标,是一个接口目标可以不同
	sink Sink
	// 保存binlog位置
	posHolder PositionHolder
	// 给分表分库的使用,先预留
	rowMapper RowMapper

	// 存储解析后的binlog
	syncCh chan interface{}

	// http服务
	http http.Server

	InsertNum sync2.AtomicInt64
	UpdateNum sync2.AtomicInt64
	DeleteNum sync2.AtomicInt64
}

func NewSyncManager(c *conf.ConfigSet, holder PositionHolder, rowMapper RowMapper, sink Sink) (*SyncManager, error) {
	sm := new(SyncManager)

	sm.c = c
	sm.syncCh = make(chan interface{}, 4096)
	sm.Ctx, sm.cancel = context.WithCancel(context.Background())

	sm.posHolder = holder
	sm.rowMapper = rowMapper
	sm.sink = sink

	var err error
	// 初始化binlog位置接口体
	if err = sm.newMaster(); err != nil {
		return nil, errors.Trace(err)
	}

	// 初始化记录binlog位置
	if err = sm.prepareMaster(); err != nil {
		return nil, errors.Trace(err)
	}

	// 初始化canal配置
	if err = sm.newCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	// 使用go-canal提供的接口
	if err = sm.prepareCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	// We must use binlog full row image,这个应该是mysql里面的知识,后期学习一下
	if err = sm.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, errors.Trace(err)
	}

	return sm, nil
}

func (s *SyncManager) newMaster() error {
	s.master = &masterInfo{}

	return nil
}

func (s *SyncManager) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", s.c.SourceDB.Host, s.c.SourceDB.Port)
	cfg.User = s.c.SourceDB.UserName
	cfg.Password = s.c.SourceDB.Password
	cfg.Charset = s.c.SourceDB.Charset
	cfg.Flavor = s.c.SourceDB.Flavor

	cfg.ServerID = s.c.SourceDB.ServerID
	cfg.Dump.ExecutionPath = ""
	cfg.Dump.DiscardErr = false
	cfg.Dump.SkipMasterData = s.c.SourceDB.SkipMasterData

	if s.c.SourceDB.DumpExec != "" {
		cfg.Dump.ExecutionPath = s.c.SourceDB.DumpExec
	}
	//cfg.SemiSyncEnabled = false

	for _, s := range s.c.SourceDB.Sources {
		for _, t := range s.Tables {
			cfg.IncludeTableRegex = append(cfg.IncludeTableRegex, s.Schema+"\\."+t)
		}
	}

	var err error

	s.canal, err = canal.NewCanal(cfg)
	return errors.Trace(err)
}

//
func (s *SyncManager) prepareCanal() error {
	s.canal.SetEventHandler(&eventHandler{s: s})
	return nil
}

// 加载binlog位置,建议使用接口外面自定义适合自己业务的加载方式,如果用docker不建议使用本地文件
func (s *SyncManager) prepareMaster() error {
	if s.posHolder == nil {
		s.posHolder = &FilePositionHolder{dataDir: s.c.SourceDB.DataDir}
	}

	s.master.holder = s.posHolder
	return s.master.loadPos()
}

func (s *SyncManager) Run() error {
	s.wg.Add(1)
	// 添加prometheus的metrics值
	canalSyncState.Set(float64(1))

	// 监听channel消息并记录存储
	go s.syncLoop()

	// 这里的值在前面的new的时候就从持久化中捞回来了
	pos := s.master.Position()
	if err := s.canal.RunFrom(pos); err != nil {
		log.Errorf("start canal err: %+v", err)
		return errors.Trace(err)
	}

	return nil
}

func (s *SyncManager) Close() {
	log.Infof("closing sync manager")
	s.cancel()
	s.canal.Close()
	s.master.Close()
	s.wg.Wait()
}
