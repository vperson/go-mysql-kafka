package gkafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bwmarrin/snowflake"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/canal"
	log "github.com/sirupsen/logrus"
	blp "go-mysql-kafka/binlog-payload"
	"go-mysql-kafka/conf"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
	"time"
)

type Kafka struct {
	c             *conf.ConfigSet
	producer      sarama.SyncProducer
	producerAsync sarama.AsyncProducer
	Async         bool
	idGen         *snowflake.Node
}

func NewKafka(c *conf.ConfigSet) (*Kafka, error) {
	PartitionerType := c.Kafka.Producer.PartitionerType
	kafkaVersion, err := sarama.ParseKafkaVersion(c.Kafka.Version)
	if err != nil {
		return nil, errors.Trace(err)
	}

	config := sarama.NewConfig()
	config.Version = kafkaVersion
	// 是否等待成功和失败后的响应,只有的RequireAcks设置不是NoReponse这里才有用
	config.Producer.Return.Successes = c.Kafka.Producer.ReturnSuccesses
	config.Producer.Return.Errors = c.Kafka.Producer.ReturnErrors

	// 开启sasl认证
	if c.Kafka.SaslEnable == true {
		certBytes, err := ioutil.ReadFile(c.Kafka.CertFile)
		if err != nil {
			return nil, err
		}

		config.Net.SASL.Enable = true
		config.Net.SASL.User = c.Kafka.Username
		config.Net.SASL.Password = c.Kafka.Password
		config.Net.SASL.Handshake = true

		clientCertPool := x509.NewCertPool()
		if ok := clientCertPool.AppendCertsFromPEM(certBytes); !ok {
			return nil, fmt.Errorf("kafka producer failed to parse root certificate")
		}

		config.Net.TLS.Config = &tls.Config{
			RootCAs:            clientCertPool,
			InsecureSkipVerify: c.Kafka.InsecureSkipVerify,
		}
		config.Net.TLS.Enable = true
	}

	// 是否需要确认消息已经正常写入
	if c.Kafka.Producer.RequiredAcks == 0 {
		config.Producer.RequiredAcks = sarama.NoResponse
	} else if c.Kafka.Producer.RequiredAcks == 1 {
		config.Producer.RequiredAcks = sarama.WaitForLocal
	} else {
		config.Producer.RequiredAcks = sarama.WaitForAll
	}

	// 选择分区类型,用的最多的也就manual: 所有消息都投递到partitioner 0 上
	switch PartitionerType {
	case "Manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
	case "RoundRobin":
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	case "Random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "Hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "ReferenceHash":
		config.Producer.Partitioner = sarama.NewReferenceHashPartitioner
	default:
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	}

	// 验证配置的可行性
	if err = config.Validate(); err != nil {
		return nil, fmt.Errorf("kafka producer config invalidate. err: %v", err)
	}

	kafka := Kafka{
		c:     c,
		Async: c.Kafka.Producer.Async,
	}
	if c.Kafka.Producer.Async {
		kafka.producerAsync, err = sarama.NewAsyncProducer(c.Kafka.Brokers, config)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		kafka.producer, err = sarama.NewSyncProducer(c.Kafka.Brokers, config)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// 初始化ID生成器
	kafka.idGen, err = snowflake.NewNode(int64(c.SourceDB.ServerID))
	if err != nil {
		return nil, fmt.Errorf("id gen init err: %+v", err)
	}

	return &kafka, nil

}

// 分析binlog生成json
func (k *Kafka) Parse(e *canal.RowsEvent) ([]interface{}, error) {
	now := time.Now()
	payload := blp.ParsePayload(e)
	payloadByte, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	var id = k.idGen.Generate().String()
	var hdrs []sarama.RecordHeader

	// 博士之前把解析出来的binlog存放在redis,这里就先不存储了

	topic := e.Table.Name

	// 判断topic是否需要特定,默认是表名
	for _, m := range k.c.Kafka.Producer.TableMapperTopic {
		if m.SourceTable == e.Table.Name && m.Topic != "" {
			topic = m.Topic
		}
	}

	// 先填写自定义头部
	for _, h := range k.c.Kafka.Producer.Headers {
		hdrs = append(hdrs, sarama.RecordHeader{
			Key:   []byte(h.Key),
			Value: []byte(h.Value),
		})
	}

	// 系统头部一个时间，一个事件ID
	hdrs = append(hdrs, []sarama.RecordHeader{
		{
			Key:   []byte("EventTriggerTime"),
			Value: []byte(strconv.FormatInt(now.Unix(), 10)),
		},
		{
			Key:   []byte("EventID"),
			Value: []byte(id),
		},
	}...)
	var message *sarama.ProducerMessage
	message = &sarama.ProducerMessage{
		Topic:     topic,
		Partition: 0,
	}
	message.Headers = hdrs
	message.Value = sarama.StringEncoder(string(payloadByte))
	return []interface{}{
		&message,
	}, nil

}

// 将kafka消息推送到kafka
func (k *Kafka) Publish(reqs []interface{}) error {
	var msgs []*sarama.ProducerMessage

	if len(reqs) > 0 {
		for _, req := range reqs {
			msgs = append(msgs, *req.(**sarama.ProducerMessage))
		}

		for _, msg := range msgs {
			// kafka异步写入,谨慎使用,kafka会挂的(´;︵;`)
			if k.Async == true {
				k.producerAsync.Input() <- msg
				go k.asyncSendMessage(msg)
			} else {
				// TODO: 可以优化成批量写入
				p, offset, err := k.producer.SendMessage(msg)
				if err != nil {
					return errors.Trace(err)
				}

				if k.c.Debug == true {
					log.Infof("sent to partition  %d at offset %d", p, offset)
				}
			}
		}
	}
	return nil
}

func (k *Kafka) SendMessageTest() {
	msg := sarama.ProducerMessage{
		Topic: "FonzieTestTopic",
	}
	msg.Value = sarama.StringEncoder("value")
	// saram对于低版本的kafka的headers不能为空
	msg.Headers = []sarama.RecordHeader{
		{
			Key: []byte("hello"),
			Value: []byte("world"),
		},
	}
	p, offset, err := k.producer.SendMessage(&msg)
	if err != nil {
		fmt.Println(errors.Trace(err))
		log.Fatalf("test send message error: %v", err)
	}
	log.Fatalf("sent to partition  %d at offset %d", p, offset)
}

func (k *Kafka) asyncSendMessage(msg *sarama.ProducerMessage) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	doneCh := make(chan struct{})
	for {

		select {
		//case k.producerAsync.Input() <- msg:
		case result := <-k.producerAsync.Successes():
			if k.c.Debug == true {
				log.Infof("message: %s sent to partition  %d at offset %d\n", result.Value, result.Partition, result.Offset)
			}
			doneCh <- struct{}{}
		case err := <-k.producerAsync.Errors():
			log.Errorf("kafka async send message err: %+v", err)
		case <-signals:
			doneCh <- struct{}{}
		}
	}
	<-doneCh

	//if k.c.Debug == true {
	//	log.Infof("kafka async send message ok")
	//}

}

func (k *Kafka) Close() {
	var err error
	if k.Async && k.producerAsync != nil {
		err = k.producerAsync.Close()
	} else if k.Async == false && k.producer != nil {
		err = k.producer.Close()
	} else {
		err = fmt.Errorf("did not close any sarama kafka")
	}

	if err != nil {
		log.Error("close kafka err: %+v", err)
	} else {
		log.Info("close kafka is ok")
	}
}
