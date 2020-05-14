package sync_manager

import (
	"bytes"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"go-mysql-kafka/conf"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

type Stat struct {
	C  *conf.ConfigSet
	Sm *SyncManager
	l  net.Listener
}

var (
	canalSyncState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "mysql2kafka_canal_state",
			Help: "The canal slave running state: 0=stopped, 1=ok",
		},
	)
	dbInsertNum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mysql_binlog_inserted_num",
			Help: "The number of docs inserted from mysql",
		}, []string{"index"},
	)
	dbUpdateNum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mysql_binlog_updated_num",
			Help: "The number of docs updated from mysql",
		}, []string{"index"},
	)
	dbDeleteNum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mysql_binlog_deleted_num",
			Help: "The number of docs deleted from mysql",
		}, []string{"index"},
	)
)

func (s *Stat) Run() {
	var err error
	s.l, err = net.Listen("tcp", s.C.Http.StatAddr)
	if err != nil {
		log.Errorf("listen stat addr %s err %v", s.C.Http.StatAddr, err)
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/stat", s)
	mux.Handle(s.C.Http.StatPath, promhttp.Handler())
	if s.C.Debug == true {
		mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
		mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
		mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
		mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
		mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	}
	srv := http.Server{
		Addr:         s.C.Http.StatAddr,
		Handler:      mux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	log.Infof("http listen : http://%s", s.C.Http.StatAddr)
	err = srv.Serve(s.l)
	defer func(err error) {
		log.Errorf("http listen err : %v", err)
		s.Sm.cancel()
	}(err)
}

// 关闭http
func (s *Stat) Close() {
	if s.l != nil {
		s.l.Close()
	}
}

func (s *Stat) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var buf bytes.Buffer

	sm := s.Sm
	rr, err := sm.canal.Execute("SHOW MASTER STATUS")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("execute sql error %v", err)))
		return
	}

	binName, _ := rr.GetString(0, 0)
	binPos, _ := rr.GetUint(0, 1)

	pos := sm.canal.SyncedPosition()

	buf.WriteString(fmt.Sprintf("sync info of debug: %v\n", sm.c.Debug))
	buf.WriteString(fmt.Sprintf("source database addr %s:%d\n", sm.c.SourceDB.Host, sm.c.SourceDB.Port))
	for _, s := range s.C.SourceDB.Sources {
		buf.WriteString(fmt.Sprintf("\nDB: %s\n", s.Schema))
		for n, t := range s.Tables {
			buf.WriteString(fmt.Sprintf("Table%d: %s\n", n+1, t))
		}
	}
	buf.WriteString(fmt.Sprintf("-------------------------------------------------------------------------------\n"))
	buf.WriteString(fmt.Sprintf("server_current_binlog:(%s, %d)\n", binName, binPos))
	buf.WriteString(fmt.Sprintf("read_binlog:%s\n", pos))

	buf.WriteString(fmt.Sprintf("insert_num:%d\n", sm.InsertNum.Get()))
	buf.WriteString(fmt.Sprintf("update_num:%d\n", sm.UpdateNum.Get()))
	buf.WriteString(fmt.Sprintf("delete_num:%d\n", sm.DeleteNum.Get()))
	buf.WriteString(fmt.Sprintf("sync chan capacity: %d\n", len(sm.syncCh)))
	buf.WriteString(fmt.Sprintf("-------------------------------------------------------------------------------\n"))
	buf.WriteString(fmt.Sprintf("kafka: %v", sm.c.Kafka.Brokers))

	w.Write(buf.Bytes())
}
