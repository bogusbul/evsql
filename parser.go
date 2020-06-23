package evsql

import (
	"github.com/xwb1989/sqlparser"
	"io"
	"log"
	"reflect"
)

func appendTable(tableExpr *sqlparser.AliasedTableExpr, tables []string) []string {
	tables = append(tables, sqlparser.GetTableName(tableExpr.Expr).String())
	return tables
}

func appendJoinTable(tableExpr *sqlparser.JoinTableExpr, tables []string) []string {
	switch reflect.TypeOf(tableExpr.LeftExpr).String() {
	case "*sqlparser.JoinTableExpr":
		tables = appendJoinTable(tableExpr.LeftExpr.(*sqlparser.JoinTableExpr), tables)
	case "*sqlparser.AliasedTableExpr":
		tables = append(tables, sqlparser.GetTableName(tableExpr.LeftExpr.(*sqlparser.AliasedTableExpr).Expr).String())
		tables = append(tables, sqlparser.GetTableName(tableExpr.RightExpr.(*sqlparser.AliasedTableExpr).Expr).String())
	}
	return tables
}

func SqlToTables(sql string) []string {
	tables := []string{}
	tokens := sqlparser.NewStringTokenizer(sql)
	for {
		stmt, err := sqlparser.ParseNext(tokens)
		if err == io.EOF {
			break
		}
		switch stmt := stmt.(type) {
		case *sqlparser.Select:
			for _, tableExpr := range stmt.From {
				switch reflect.TypeOf(tableExpr).String() {
				case "*sqlparser.JoinTableExpr":
					tables = appendJoinTable(tableExpr.(*sqlparser.JoinTableExpr), tables)
				case "*sqlparser.AliasedTableExpr":
					tables = appendTable(tableExpr.(*sqlparser.AliasedTableExpr), tables)
				}
			}
		}
	}
	log.Println("TABLES", tables)
	return tables
}
