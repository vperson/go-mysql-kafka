package conf

import (
	"github.com/BurntSushi/toml"
	"github.com/siddontang/go-mysql/schema"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"time"
)

type ConfigSet struct {
	Debug    bool      `toml:"debug"`    // 是否开启debug模式
	Env      string    `toml:"env"`      // 运行环境
	SourceDB *MysqlSet `toml:"sourceDB"` // 源数据库的配置
	Http     *HttpSet  `toml:"http"`     // http配置
	Redis    *RedisSet `toml:"redis"`    // redis配置
	Mapper   *Mapper   `toml:"mapper"`   // 分表分库匹配规则
	Kafka    *KafkaSet `toml:"kafka"`
}

// 分表分库
type Mapper struct {
	Schemas []string `toml:"schemas"`
}

type HttpSet struct {
	StatAddr string `toml:"statAddr"`
	StatPath string `toml:"statPath"` // metrics 访问路劲
}

type KafkaSet struct {
	Brokers  []string          `toml:"brokers"`
	Version  string            `toml:"version"` // kafka的版本
	Producer *KafkaProducerSet `toml:"producer"`

	InsecureSkipVerify bool   `toml:"insecureSkipVerify"`
	SaslEnable         bool   `toml:"saslEnable"`
	Username           string `toml:"username"`
	Password           string `toml:"password"`
	CertFile           string `toml:"certFile"`
}

type KafkaProducerSet struct {
	RequiredAcks     int                 `toml:"requiredAcks"`
	ReturnSuccesses  bool                `toml:"returnSuccesses"`
	ReturnErrors     bool                `toml:"returnErrors"`
	Async            bool                `toml:"async"`
	RetryMax         int                 `toml:"retryMax"`
	Headers          []*KafkaHeader      `toml:"headers"`
	TableMapperTopic []*KafkaMapperTopic `toml:"mapper"`
	PartitionerType  string              `toml:"partitionerType"`
}

type KafkaMapperTopic struct {
	Topic       string `toml:"topic"`
	SourceTable string `toml:"sourceTable"`
}

type KafkaHeader struct {
	Key   string `toml:"key"`
	Value string `toml:"value"`
}

type MysqlSet struct {
	Host           string        `toml:"host"`
	Port           int           `toml:"port"`
	UserName       string        `toml:"username"`
	Password       string        `toml:"password"`
	Charset        string        `toml:"charset"`
	ServerID       uint32        `toml:"serverID"`
	Flavor         string        `toml:"flavor"`    // mysql or mariadb
	DumpExec       string        `toml:"mysqldump"` // if not set or empty, ignore mysqldump.
	BulkSize       int           `toml:"bulkSize"`  // minimal items to be inserted in one bulk
	FlushBulkTime  time.Duration `toml:"flushBulkTime"`
	SkipNoPkTable  bool          `toml:"skipNoPkTable"`
	SkipMasterData bool          `toml:"skipMasterData"`
	DataDir        string        `toml:"dataDir"`

	Sources []SourceConfig `toml:"sources"`
	//Rules  []*RuleConfig  `toml:"rule"`
}

type RedisSet struct {
	Host          string        `toml:"host"`
	Password      string        `toml:"password"`
	MaxIdle       int           `toml:"maxIdle"`
	MaxActive     int           `toml:"maxActive"`
	IdleTimeout   time.Duration `toml:"idleTimeout"`
	BinlogTimeout int           `toml:"binlogTimeout"`
	BinlogPrefix  string        `toml:"binlogPrefix"`
}

type SourceConfig struct {
	Schema string   `toml:"schema"`
	Tables []string `toml:"tables"`
}

type RuleConfig struct {
	Schema string   `toml:"schema"`
	Table  string   `toml:"table"`
	Index  string   `toml:"index"`
	Type   string   `toml:"type"`
	Parent string   `toml:"parent"`
	ID     []string `toml:"id"`

	// Default, a MySQL table field name is mapped to Elasticsearch field name.
	// Sometimes, you want to use different name, e.g, the MySQL file name is title,
	// but in Elasticsearch, you want to name it my_title.
	FieldMapping map[string]string `toml:"field"`

	// MySQL table information
	TableInfo *schema.Table

	//only MySQL fields in filter will be synced , default sync all fields
	Filter []string `toml:"filter"`

	// Elasticsearch pipeline
	// To pre-process documents before indexing
	Pipeline string `toml:"pipeline"`
}

var Config = &ConfigSet{}

func Setup() {
	configPath := "app.toml"
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		log.Fatalf("read toml config err: %+v", err)
	}

	if _, err := toml.Decode(string(data), &Config); err != nil {
		log.Fatalf("decode toml config err: %+v", err)
	}

	// redis 配置
	Config.Redis.IdleTimeout = Config.Redis.IdleTimeout * time.Second

	// 数据源配置
	Config.SourceDB.FlushBulkTime = Config.SourceDB.FlushBulkTime * time.Second

	//fmt.Printf("%+v", Config)
}

// 检查配置文件
func (c *ConfigSet) checkConfig() {
	// kafka配置检查
	if len(c.Kafka.Brokers) < 1 {
		log.Fatalf("kafka brokers can not be empty, err")
	}

}
