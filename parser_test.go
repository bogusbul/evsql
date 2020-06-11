package evsql

import (
	"testing"
)

func Test_Unit_SQLParser(t *testing.T) {
	sql := "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate FROM Orders INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;"
	SqlToTables(sql)
}
