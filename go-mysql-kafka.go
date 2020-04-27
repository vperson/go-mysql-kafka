package main

import (
	log "github.com/sirupsen/logrus"
	"go-mysql-kafka/conf"
	"go-mysql-kafka/gkafka"
	"go-mysql-kafka/gredis"
	"go-mysql-kafka/holder"
	"go-mysql-kafka/mapper"
	"go-mysql-kafka/sync_manager"
	"os"
	"os/signal"
	"syscall"
)

func init() {
	conf.Setup()
	gredis.Setup()
}

func main() {
	c := conf.Config

	// 创建一个信号chan
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGKILL)

	// 初始化存储binlog位置, 这里用的是redis存储
	positionHolder, err := holder.NewPosition(c)
	if err != nil {
		log.Fatalf("init position holder err: %+v", err)
	}
	kafkaProducer, err := gkafka.NewKafka(c)
	if err != nil {
		log.Fatalf("init kafka producer err: %+v", err)
	}

	//kafkaProducer.SendMessageTest()

	// 初始化分表分库的配置
	rowsMapper := mapper.NewDRDSMapper(c)

	var sm *sync_manager.SyncManager

	sm, err = sync_manager.NewSyncManager(c, positionHolder, rowsMapper, kafkaProducer)
	if err != nil {
		log.Fatalf("init sync manager err: %+v", err)
	}

	done := make(chan struct{}, 1)

	go func() {
		sm.Run()

		done <- struct{}{}
		log.Infof("run end")

	}()

	// http服务
	st := &sync_manager.Stat{Sm: sm, C: c}
	go st.Run()


	select {
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
		//TODO 临时写一下，之后应该把多个manager等context归总到一个进行监听
	case <-sm.Ctx.Done():
		log.Infof("context is done with %v, closing", sm.Ctx.Err())
	}


	sm.Close()
	kafkaProducer.Close()
	gredis.Close()
	<-done
	log.Infof("sync manager is stop")
}
