package evsql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/go-sql-driver/mysql"
)

var mux sync.Mutex

type MySQL struct {
	DB       *sql.DB
	User     string
	Pass     string
	Address  string
	Database string
	Tables   map[string][]string
	Tmpl     string
}

func NewMySQL(address, user, pass, database string) (*MySQL, error) {
	m := &MySQL{}
	m.User = user
	m.Pass = pass
	m.Address = address
	m.Database = database
	dsn := user + ":" + pass + "@tcp(" + address + ")/" + database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	m.DB = db
	return m, nil
}

func (m *MySQL) Close() error {
	return m.DB.Close()
}

func (m *MySQL) Query(query string) ([]map[string]interface{}, error) {
	//defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	go func(ctx context.Context) {
		select {
		case <-time.After(300 * time.Second):
			log.Println("overslept")
		case <-ctx.Done():
			log.Println(ctx.Err())
		}
	}(ctx)
	//time.Sleep(20*time.Second)
	conn, err := m.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	stmt, err := conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	cTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	dest := []interface{}{}
	resMap := []map[string]interface{}{}
	for _, cType := range cTypes {
		//log.Println(cType.ScanType().String())
		switch cType.ScanType().String() {
		case "sql.RawBytes":
			dest = append(dest, new(sql.RawBytes))
		case "uint32":
			dest = append(dest, new(uint32))
		case "mysql.NullTime":
			dest = append(dest, new(mysql.NullTime))
		case "sql.NullInt64":
			dest = append(dest, new(sql.NullInt64))
		default:
			dest = append(dest, new(interface{}))
		}
	}
	counter := -1
	for rows.Next() {
		counter++
		err = rows.Scan(dest...)
		if err != nil {
			return nil, err
		}
		if len(resMap)-1 < counter {
			resMap = append(resMap, map[string]interface{}{})
		}
		for i := 0; i < len(dest); i++ {
			switch reflect.TypeOf(dest[i]).String() {
			case "*sql.RawBytes":
				resMap[counter][columns[i]] = string(*dest[i].(*sql.RawBytes))
			case "*uint32":
				resMap[counter][columns[i]] = strconv.FormatUint(uint64(*dest[i].(*uint32)), 10)
			case "*mysql.NullTime":
				resMap[counter][columns[i]] = dest[i].(*mysql.NullTime)
			case "*sql.NullInt64":
				resMap[counter][columns[i]] = strconv.FormatInt(dest[i].(*sql.NullInt64).Int64, 10)
			default:
				resMap[counter][columns[i]] = dest[i].(*interface{})
			}
		}
	}
	log.Println(conn.Close())
	return resMap, nil
}

func (m *MySQL) PreparedQuery(query string) ([]map[string]map[string]interface{}, error) {
	query, err := m.StatementPrepare(query)
	if err != nil {
		return nil, err
	}
	log.Println("query", query)
	//defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	go func(ctx context.Context) {
		select {
		case <-time.After(300 * time.Second):
			log.Println("overslept")
		case <-ctx.Done():
			log.Println(ctx.Err())
		}
	}(ctx)
	//time.Sleep(20*time.Second)
	conn, err := m.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	stmt, err := conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	cTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	dest := []interface{}{}
	resMap := []map[string]map[string]interface{}{}
	for _, cType := range cTypes {
		mux.Lock()
		switch cType.ScanType().String() {
		case "sql.RawBytes":
			dest = append(dest, new(sql.RawBytes))
		case "uint32":
			dest = append(dest, new(uint32))
		case "mysql.NullTime":
			dest = append(dest, new(mysql.NullTime))
		case "sql.NullInt64":
			dest = append(dest, new(sql.NullInt64))
		// case "sql.NullFloat64":
		// 	dest = append(dest, new(sql.NullFloat64))
		case "int64":
			dest = append(dest, new(int64))
		case "int32":
			dest = append(dest, new(int32))
		case "int8":
			dest = append(dest, new(int8))
		default:
			dest = append(dest, new(interface{}))
		}
		mux.Unlock()
	}
	counter := -1
	for rows.Next() {
		counter++
		err = rows.Scan(dest...)
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(dest); i++ {
			tableCol := strings.Split(columns[i], ".")
			mux.Lock()
			if (len(resMap) - 1) < counter {
				resMap = append(resMap, map[string]map[string]interface{}{tableCol[0]: map[string]interface{}{}})
			}
			if _, ok := resMap[counter][tableCol[0]]; !ok {
				resMap[counter][tableCol[0]] = map[string]interface{}{tableCol[0]: map[string]interface{}{}}
			}
			switch reflect.TypeOf(dest[i]).String() {
			case "*sql.RawBytes":
				resMap[counter][tableCol[0]][tableCol[1]] = string(*dest[i].(*sql.RawBytes))
			case "*uint32":
				resMap[counter][tableCol[0]][tableCol[1]] = strconv.FormatUint(uint64(*dest[i].(*uint32)), 10)
			case "*mysql.NullTime":
				if dest[i].(*mysql.NullTime).Valid {
					resMap[counter][tableCol[0]][tableCol[1]] = (dest[i].(*mysql.NullTime).Time).Format("2006-01-02 15:04:05")
				} else {
					resMap[counter][tableCol[0]][tableCol[1]] = ""
				}
			case "*sql.NullInt64":
				resMap[counter][tableCol[0]][tableCol[1]] = strconv.FormatInt(dest[i].(*sql.NullInt64).Int64, 10)
			// case "*sql.NullFloat64":
			// 	log.Println(dest[i].(*sql.NullFloat64).Float64)
			// 	log.Println(strconv.FormatFloat(dest[i].(*sql.NullFloat64), 'E', -1, 64)
			case "*int64":
				resMap[counter][tableCol[0]][tableCol[1]] = *dest[i].(*int64)
			case "*int32":
				resMap[counter][tableCol[0]][tableCol[1]] = *dest[i].(*int32)
			case "*int8":
				resMap[counter][tableCol[0]][tableCol[1]] = *dest[i].(*int8)
			default:
				resMap[counter][tableCol[0]][tableCol[1]] = dest[i].(*interface{})
			}
			mux.Unlock()
		}
	}
	log.Println(conn.Close())
	return resMap, nil
}

func generateWildcardColumns(table string, fields []string) string {
	wildcardColumns := ""
	for _, field := range fields {
		wildcardColumns += table + "." + field + " AS '" + table + "." + field + "',"
	}
	return strings.TrimRight(wildcardColumns, ",")
}

func (m *MySQL) StatementPrepare(query string) (string, error) {
	m.Tables = map[string][]string{}
	tables := SqlToTables(query)
	queryNew := query
	if len(tables) > 0 {
		queryNew = strings.Replace(query, "*", "{{generateAllWildcardColumns .}}", 1)
	} else {
		return "", errors.New("no tables found for given query :" + query)
	}
	for _, table := range tables {
		query := `SHOW COLUMNS FROM ` + table
		r, err := m.Query(query)
		if err != nil {
			return "", err
		}
		mux.Lock()
		for _, value := range r {
			for cKey, cValue := range value {
				if cKey == "Field" {
					if _, ok := m.Tables[table]; !ok {
						m.Tables[table] = []string{}
					}
					m.Tables[table] = append(m.Tables[table], cValue.(string))
				}
			}
		}
		mux.Unlock()
	}
	buff := bytes.NewBuffer(nil)
	f := template.FuncMap{
		"generateWildcardColumns": generateWildcardColumns,
		"generateAllWildcardColumns": func(tables map[string][]string) string {
			wildcardColumns := ""
			for table, fields := range tables {
				wildcardColumns += generateWildcardColumns(table, fields) + ","
			}
			return strings.TrimRight(wildcardColumns, ",")
		},
	}
	tmpl, err := template.New("statement").Funcs(f).Parse(queryNew)
	if err != nil {
		return "", err
	}
	err = tmpl.ExecuteTemplate(buff, "statement", m.Tables)
	if err != nil {
		return "", err
	}
	return buff.String(), nil
}
