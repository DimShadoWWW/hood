package hood

import (
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"time"
)

func init() {
	RegisterDialect("sqlite3", NewSqlite3())
}

type sqlite3 struct {
	base
}

func NewSqlite3() Dialect {
	d := &sqlite3{}
	d.base.Dialect = d
	return d
}

func (d *sqlite3) SqlType(f interface{}, size int) string {
	switch f.(type) {
	case Id:
		return "INTEGER"
	case time.Time, Created, Updated:
		return "INTEGER"
	case bool:
		return "BOOLEAN"
	case int, int8, int16, int32, uint, uint8, uint16, uint32:
		return "INTEGER"
	case int64, uint64:
		return "INTEGER"
	case float32, float64:
		return "FLOAT"
	case []byte:
		return "BLOB"
	case string:
		if size > 0 && size < 65532 {
			return fmt.Sprintf("VARCHAR(%d)", size)
		}
		return "TEXT"
	}
	panic("invalid sql type")
}

func (d *sqlite3) Insert(hood *Hood, model *Model) (Id, error) {
	sql, args := d.Dialect.InsertSql(model)
	var id int64
	err := hood.QueryRow(sql, args...).Scan(&id)
	return Id(id), err
}

func (d *sqlite3) InsertSql(model *Model) (string, []interface{}) {
	m := 0
	columns, markers, values := columnsMarkersAndValuesForModel(d.Dialect, model, &m)
	quotedColumns := make([]string, 0, len(columns))
	for _, c := range columns {
		quotedColumns = append(quotedColumns, d.Dialect.Quote(c))
	}
	sql := fmt.Sprintf(
		"INSERT INTO %v (%v) VALUES (%v) RETURNING %v",
		d.Dialect.Quote(model.Table),
		strings.Join(quotedColumns, ", "),
		strings.Join(markers, ", "),
		d.Dialect.Quote(model.Pk.Name),
	)
	return sql, values
}

func (d *sqlite3) KeywordAutoIncrement() string {
	// postgres has not auto increment keyword, uses SERIAL type
   return "PRIMARY KEY"
	//return ""
}
