package holder

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"go-mysql-kafka/conf"
	"go-mysql-kafka/gredis"
	"regexp"
	"strconv"
	"strings"
)
// 覆盖默认的读取和存储binlog的函数

type PositionHolder struct {
	// binlog超时时间
	binlogTimeout int
	// redis存储前缀
	prefix string
	// 一般是环境
	label string
}

func NewPosition(c *conf.ConfigSet) (*PositionHolder, error) {
	return &PositionHolder{
		binlogTimeout: c.Redis.BinlogTimeout,
		prefix:        c.Redis.BinlogPrefix,
		label:         c.Env,
	}, nil
}

func (p *PositionHolder) Save(pos *mysql.Position) error {
	key := fmt.Sprintf("%s:%s", p.prefix, p.label)
	value := fmt.Sprintf("%s:%v", pos.Name, pos.Pos)

	return gredis.Set(key, value, 0)

}

func (p *PositionHolder) Load() (*mysql.Position, error) {
	var pos mysql.Position
	key := fmt.Sprintf("%s:%s", p.prefix, p.label)
	posval, err := gredis.GetString(key)
	if err == redis.ErrNil {
		return nil, nil
	}

	//posval := string(posvalByte)
	re := regexp.MustCompile("\"")
	posval = re.ReplaceAllString(posval, "")
	toks := strings.Split(posval, ":")
	if len(toks) == 2 {
		pos.Name = toks[0]
		rawPos, err := strconv.Atoi(toks[1])
		if err != nil {
			return nil, err
		}

		pos.Pos = uint32(rawPos)
		return &pos, errors.Trace(err)
	}
	return nil, fmt.Errorf("cannot parse mysql position")
}
