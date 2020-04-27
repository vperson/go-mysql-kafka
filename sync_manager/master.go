package sync_manager

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/ioutil2"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type masterInfo struct {
	sync.RWMutex

	Name string
	Pos  uint32

	filePath     string
	lastSaveTime time.Time

	holder PositionHolder
}

// 默认从文件中加载
type FilePositionHolder struct {
	dataDir string
}

// Save the function to save the binlog position
func (h *FilePositionHolder) Save(pos *mysql.Position) error {
	if len(h.dataDir) == 0 {
		return nil
	}

	filePath := path.Join(h.dataDir, "master.info")

	posContent := fmt.Sprintf("%s:%v", pos.Name, pos.Pos)

	var err error
	if err = ioutil2.WriteFileAtomic(filePath, []byte(posContent), 0644); err != nil {
		log.Errorf("canal save master info to file %s err %v", filePath, err)
	}
	return err
}

// Load the function to retrieve the MySQL binlog position
func (h *FilePositionHolder) Load() (*mysql.Position, error) {
	var pos mysql.Position

	if err := os.MkdirAll(h.dataDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}

	filePath := path.Join(h.dataDir, "master.info")
	f, err := os.Open(filePath)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		return nil, nil
	}
	defer f.Close()

	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	toks := strings.Split(string(bytes), ":")
	if len(toks) == 2 {
		pos.Name = toks[0]

		rawPos, err := strconv.Atoi(toks[1])

		if err != nil {
			return nil, err
		}
		pos.Pos = uint32(rawPos)
		return &pos, errors.Trace(err)
	}
	return nil, errors.New("Cannot parse mysql position")
}

// 使用接口实现,这样方便数据保存到文件或者redis中
type PositionHolder interface {
	// 加载binlog位置函数
	Load() (*mysql.Position, error)
	// 保存binlog位置函数
	Save(pos *mysql.Position) error
}

func (m *masterInfo) loadPos() error {
	m.lastSaveTime = time.Now()

	pos, err := m.holder.Load()
	if err != nil {
		return errors.Trace(err)
	}

	if pos != nil {
		m.Name = pos.Name
		m.Pos = pos.Pos
	}

	return nil
}

func (m *masterInfo) Save(pos mysql.Position) error {
	log.Infof("save position %s", pos)

	m.Lock()
	defer m.Unlock()

	m.Name = pos.Name
	m.Pos = pos.Pos

	now := time.Now()
	if now.Sub(m.lastSaveTime) < time.Second {
		return nil
	}
	m.lastSaveTime = now

	err := m.holder.Save(&pos)
	return errors.Trace(err)
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return mysql.Position{
		Name: m.Name,
		Pos:  m.Pos,
	}
}

func (m *masterInfo) Close() error {
	pos := m.Position()

	return m.Save(pos)
}
