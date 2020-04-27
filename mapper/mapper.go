package mapper

import (
	"github.com/siddontang/go-mysql/canal"
	"go-mysql-kafka/conf"
	"regexp"
)

// 这块用于处理分表分库逻辑,这里只处理了drds的分表分库

type MultiSourceMapper struct {
	c      *conf.ConfigSet
	tabPat *regexp.Regexp
}

func NewDRDSMapper(c *conf.ConfigSet) *MultiSourceMapper {
	return &MultiSourceMapper{
		c:      c,
		tabPat: regexp.MustCompile(`\d+$`),
	}
}

// 处理分表分库的逻辑
func (m *MultiSourceMapper) Transform(e *canal.RowsEvent) *canal.RowsEvent {
	//for _, name := range m.c.Mapper.Schemas {
	//	if strings.HasPrefix(e.Table.Schema, name) {
	//		e.Table.Schema = name
	//
	//		if m.tabPat.MatchString(e.Table.Name) {
	//			e.Table.Name = m.tabPat.ReplaceAllString(e.Table.Name, "")
	//		}
	//	}
	//}

	return e
}
