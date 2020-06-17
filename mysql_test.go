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
	results, err := m.PreparedQuery("SELECT * FROM user")
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
