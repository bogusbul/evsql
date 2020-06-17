package evsql

import (
	"bytes"
	"context"
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"log"
	"reflect"
	"strings"
	"text/template"
	"time"
)

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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go func(ctx context.Context) {
		select {
		case <-time.After(16 * time.Second):
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
				nv := new(sql.RawBytes)
				*nv = *dest[i].(*sql.RawBytes)
				resMap[counter][columns[i]] = nv
			case "*uint32":
				nv := new(uint32)
				*nv = *dest[i].(*uint32)
				resMap[counter][columns[i]] = nv

			case "*mysql.NullTime":
				nv := new(mysql.NullTime)
				*nv = *dest[i].(*mysql.NullTime)
				resMap[counter][columns[i]] = nv

			case "*sql.NullInt64":
				nv := new(sql.NullInt64)
				*nv = *dest[i].(*sql.NullInt64)
				resMap[counter][columns[i]] = nv

			default:
				nv := new(interface{})
				*nv = *dest[i].(*interface{})
				resMap[counter][columns[i]] = nv

			}
		}
	}
	log.Println(conn.Close())
	return resMap, nil
}

func (m *MySQL) PreparedQuery(query string) ([]map[string]map[string]interface{}, error) {
	if len(m.Tables) == 0 {
		// if no tables aber defined we need to do so otherwise we will not be able to return the result map back
		nQuery, err := m.StatementPrepare(query)
		if err != nil {
			return nil, err
		}
		// replace the query
		query = nQuery
	}
	log.Println("query", query)
	//defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go func(ctx context.Context) {
		select {
		case <-time.After(16 * time.Second):
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
		for i := 0; i < len(dest); i++ {
			tableCol := strings.Split(columns[i], ".")
			if (len(resMap) - 1) < counter {
				resMap = append(resMap, map[string]map[string]interface{}{tableCol[0]: map[string]interface{}{}})
			}
			switch reflect.TypeOf(dest[i]).String() {
			case "*sql.RawBytes":
				nv := new(sql.RawBytes)
				*nv = *dest[i].(*sql.RawBytes)
				resMap[counter][tableCol[0]][tableCol[1]] = nv

			case "*uint32":
				nv := new(uint32)
				*nv = *dest[i].(*uint32)
				resMap[counter][tableCol[0]][tableCol[1]] = nv

			case "*mysql.NullTime":
				nv := new(mysql.NullTime)
				*nv = *dest[i].(*mysql.NullTime)
				resMap[counter][tableCol[0]][tableCol[1]] = nv

			case "*sql.NullInt64":
				nv := new(sql.NullInt64)
				*nv = *dest[i].(*sql.NullInt64)
				resMap[counter][tableCol[0]][tableCol[1]] = nv

			default:
				nv := new(interface{})
				*nv = *dest[i].(*interface{})
				resMap[counter][tableCol[0]][tableCol[1]] = nv

			}
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
	queryNew := strings.Replace(query, "*", "{{generateAllWildcardColumns .}}", 1)
	for _, table := range tables {
		query := `SHOW COLUMNS FROM ` + table
		r, err := m.Query(query)
		if err != nil {
			return "", err
		}
		for _, value := range r {
			for cKey, cValue := range value {
				if cKey == "Field" {
					val := cValue.(*sql.RawBytes)
					if _, ok := m.Tables[table]; !ok {
						m.Tables[table] = []string{}
					}
					m.Tables[table] = append(m.Tables[table], string(*val))
				}
			}
		}
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
