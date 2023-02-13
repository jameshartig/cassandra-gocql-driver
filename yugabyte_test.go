//go:build yugabyte
// +build yugabyte

package gocql

import (
	"bytes"
	"strconv"
	"testing"
)

func TestGetKeyspaceMetadata(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	keyspaceMetadata, err := session.KeyspaceMetadata("gocql_test")
	if err != nil {
		t.Fatalf("failed to query the keyspace metadata with err: %v", err)
	}
	if keyspaceMetadata == nil {
		t.Fatal("failed to query the keyspace metadata, nil returned")
	}
	if keyspaceMetadata.Name != "gocql_test" {
		t.Errorf("Expected keyspace name to be 'gocql' but was '%s'", keyspaceMetadata.Name)
	}
	if keyspaceMetadata.StrategyClass != "org.apache.cassandra.locator.SimpleStrategy" {
		t.Errorf("Expected replication strategy class to be 'org.apache.cassandra.locator.SimpleStrategy' but was '%s'", keyspaceMetadata.StrategyClass)
	}
	if keyspaceMetadata.StrategyOptions == nil {
		t.Error("Expected replication strategy options map but was nil")
	}
	rfStr, ok := keyspaceMetadata.StrategyOptions["replication_factor"]
	if !ok {
		t.Fatalf("Expected strategy option 'replication_factor' but was not found in %v", keyspaceMetadata.StrategyOptions)
	}
	rfInt, err := strconv.Atoi(rfStr.(string))
	if err != nil {
		t.Fatalf("Error converting string to int with err: %v", err)
	}
	if rfInt != *flagRF {
		t.Errorf("Expected replication factor to be %d but was %d", *flagRF, rfInt)
	}
}

func TestJSONB(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	defer func() {
		err := createTable(session, "DROP TABLE IF EXISTS gocql_test.jsonb")
		if err != nil {
			t.Logf("failed to delete jsonb table: %v", err)
		}
	}()

	if err := createTable(session, "CREATE TABLE gocql_test.jsonb (id int, my_jsonb jsonb, PRIMARY KEY (id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}

	b := session.NewBatch(LoggedBatch)
	b.Query("INSERT INTO gocql_test.jsonb(id, my_jsonb) VALUES (?,?)", 1, []byte("true"))
	b.Query("INSERT INTO gocql_test.jsonb(id, my_jsonb) VALUES (?,?)", 2, []byte(`{"foo":"bar"}`))

	if err := session.ExecuteBatch(b); err != nil {
		t.Fatalf("query failed. %v", err)
	} else {
		if b.Attempts() < 1 {
			t.Fatal("expected at least 1 attempt, but got 0")
		}
		if b.Latency() <= 0 {
			t.Fatalf("expected latency to be greater than 0, but got %v instead.", b.Latency())
		}
	}

	var id int
	var myJSONB []byte
	if err := session.Query("SELECT id, my_jsonb FROM gocql_test.jsonb WHERE id = 1;").Scan(&id, &myJSONB); err != nil {
		t.Fatalf("Failed to select with err: %v", err)
	} else if id != 1 {
		t.Fatalf("Expected id = 1, got %v", id)
	} else if !bytes.Equal(myJSONB, []byte("true")) {
		t.Fatalf("Expected my_jsonb = true, got %v", string(myJSONB))
	}

	if err := session.Query("SELECT id, my_jsonb FROM gocql_test.jsonb WHERE id = 2;").Scan(&id, &myJSONB); err != nil {
		t.Fatalf("Failed to select with err: %v", err)
	} else if id != 2 {
		t.Fatalf("Expected id = 2, got %v", id)
	} else if !bytes.Equal(myJSONB, []byte(`{"foo":"bar"}`)) {
		t.Fatalf(`Expected my_jsonb = {"foo":"bar"}, got %v`, string(myJSONB))
	}

	if rd, err := session.Query("SELECT id, my_jsonb FROM gocql_test.jsonb;").Iter().RowData(); err != nil {
		t.Fatalf("Failed to select with err: %v", err)
	} else if len(rd.Columns) != 2 || rd.Columns[0] != "id" || rd.Columns[1] != "my_jsonb" {
		t.Fatalf("Expected [id, my_jsonb], got %v", rd.Columns)
	} else if len(rd.Values) != 2 {
		t.Fatalf("Expected 2 values, got %v", rd.Values)
	} else if _, ok := rd.Values[0].(*int); !ok {
		t.Fatalf("Expected values[0] = *int, got %T", rd.Values[0])
	} else if _, ok := rd.Values[1].(*[]byte); !ok {
		t.Fatalf("Expected values[1] = *[]byte, got %T", rd.Values[1])
	}
}
