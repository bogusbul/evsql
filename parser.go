package evsql

import (
	"github.com/xwb1989/sqlparser"
	"io"
	"log"
	"reflect"
)

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
					switch reflect.TypeOf(tableExpr.(*sqlparser.JoinTableExpr).LeftExpr).String() {
					case "*sqlparser.JoinTableExpr":
						tables = append(tables, sqlparser.GetTableName(tableExpr.(*sqlparser.JoinTableExpr).LeftExpr.(*sqlparser.JoinTableExpr).LeftExpr.(*sqlparser.AliasedTableExpr).Expr).String())
						tables = append(tables, sqlparser.GetTableName(tableExpr.(*sqlparser.JoinTableExpr).LeftExpr.(*sqlparser.JoinTableExpr).RightExpr.(*sqlparser.AliasedTableExpr).Expr).String())
					case "*sqlparser.AliasedTableExpr":
						tables = append(tables, sqlparser.GetTableName(tableExpr.(*sqlparser.JoinTableExpr).LeftExpr.(*sqlparser.AliasedTableExpr).Expr).String())
					}
					tables = append(tables, sqlparser.GetTableName(tableExpr.(*sqlparser.JoinTableExpr).RightExpr.(*sqlparser.AliasedTableExpr).Expr).String())
				case "*sqlparser.AliasedTableExpr":
					tables = append(tables, sqlparser.GetTableName(tableExpr.(*sqlparser.AliasedTableExpr).Expr).String())
				}
			}
		}
	}
	log.Println("TABLES", tables)
	return tables
}
