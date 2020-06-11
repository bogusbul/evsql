package evsql

import (
	"encoding/json"
	"testing"
)

func Test_MysqlConnect(t *testing.T) {
	m, err := NewMySQL("localhost:8889", "root", "root", "mysql")
	if err != nil {
		t.Error(err)
	}
	query, err := m.StatementPrepare("SELECT * FROM user")
	if err != nil {
		t.Error(err)
	}
	t.Log(query)
	results, err := m.Query(query)
	if err != nil {
		t.Error(err)
	}
	for _, res := range results {
		b, e := json.Marshal(res)
		if e != nil {
			t.Error(e)
		}
		t.Log("RESULT-ROW", string(b))
	}
}
