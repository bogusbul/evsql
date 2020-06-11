# evsql

### license
BSD 1-Clause

## depends
- database/sql
- github.com/go-sql-driver/mysql
- github.com/xwb1989/sqlparser
- gnu make

## usage
get tables from query
```
tables := SqlToTables("SELECT * FROM user")
// will return []string{"user"}
```
run a query
```
m, err := NewMySQL("localhost:8889", "root", "root", "mysql")
if err != nil {
	t.Error(err)
}
results,err := m.Query("SELECT * FROM user")
if err != nil {
	t.Error(err)
}
// results is []map[string]map[string]interface{} every row is a map["{table_name}"]["column_name"]interface{}{"{value}"}
// you can just use results[0]["table_name"]["column_name"] to retrieve the value
```
because mysql does not return all columns from different tables if they have the same name we need to prepare a statement to do so
```
m, err := NewMySQL("localhost:8889", "root", "root", "mysql")
if err != nil {
	t.Error(err)
}
query, err := m.StatementPrepare("SELECT * FROM user")
if err != nil {
	t.Error(err)
}
// at this point query will be something like
// SELECT user.name AS 'user.name', user.pass AS 'user.pass' FROM user
// all available user columns will replace the * from the query
results,err := m.Query(query)
if err != nil {
	t.Error(err)
}
// results is []map[string]map[string]interface{} every row is a map["{table_name}"]["column_name"]interface{}{"{value}"}
// you can just use results[0]["table_name"]["column_name"] to retrieve the value
```

