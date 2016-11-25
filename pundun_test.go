package pundun

import (
	"bytes"
	"log"
	"testing"
	"time"
)

func TestRun1(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	session, err := Connect("127.0.0.1:8887", "admin", "admin")
	if err != nil {
		log.Println(err)
		return
	}
	defer Disconnect(session)

	tableName := "ct"
	keyDef := []string{"imsi", "ts"}
	options := map[string]interface{}{
		"type":               "leveldb",
		"data_model":         "array",
		"comparator":         "descending",
		"time_series":        false,
		"shards":             8,
		"distributed":        true,
		"replication_factor": 1,
		"hash_exclude":       []string{"ts"},
	}

	log.Println("Create Table")
	res, err := CreateTable(session, tableName, keyDef, options)
	log.Printf("Result: %v\n", res)

	log.Println("Table Info")
	res, err = TableInfo(session, tableName, []string{"type", "key"})
	log.Printf("Result: %v\n", res)

	log.Println("Close Table")
	res, err = CloseTable(session, tableName)
	log.Printf("Result: %v\n", res)

	log.Println("Open Table")
	res, err = OpenTable(session, tableName)
	log.Printf("Result: %v\n", res)

	time_ := time.Now()
	ts, _ := time_.MarshalBinary()
	key := map[string]interface{}{
		"imsi": "123456789012345",
		"ts":   ts,
	}
	columns := map[string]interface{}{
		"name":    "John",
		"counter": 1,
		"bin":     []byte{0, 0, 0, 1},
		"bool":    true,
		"double":  5.5,
	}
	var treshold uint32 = 2
	var setvalue uint32 = 1
	upOp := []UpdateOperation{
		UpdateOperation{
			Field:        "new_counter_1",
			Instruction:  Increment,
			Value:        1,
			DefaultValue: 0,
			Treshold:     &treshold,
			SetValue:     &setvalue},
		UpdateOperation{
			Field:       "new_counter_2",
			Instruction: Increment,
			Value:       1,
			Treshold:    &treshold,
			SetValue:    &setvalue},
		UpdateOperation{
			Field:       "counter",
			Instruction: Increment,
			Value:       1},
	}

	log.Println("Write")
	res, err = Write(session, tableName, key, columns)
	log.Printf("Result: %v\n", res)

	log.Println("Update")
	res, err = Update(session, tableName, key, upOp)
	log.Printf("Result: %v\n", res)

	log.Println("Read")
	res, err = Read(session, tableName, key)
	log.Printf("Result: %v\n", res)

	stime_ := time.Now()
	sts, _ := stime_.MarshalBinary()
	skey := map[string]interface{}{
		"imsi": "123456789012345",
		"ts":   sts,
	}

	log.Println("Read Range")
	res, err = ReadRange(session, tableName, skey, key, 100)
	log.Printf("Result: %v\n", res)

	log.Println("Read Range N")
	res, err = ReadRangeN(session, tableName, skey, 2)
	log.Printf("Result: %v\n", res)

	log.Println("First")
	res, err = First(session, tableName)
	log.Printf("Result: %v\n", res)

	log.Println("Last")
	res, err = Last(session, tableName)
	log.Printf("Result: %v\n", res)

	log.Println("Seek")
	res, err = Seek(session, tableName, key)
	log.Printf("Result: %v\n", res)

	log.Println("Delete")
	res, err = Delete(session, tableName, key)
	log.Printf("Result: %v\n", res)

	log.Println("Delete non existing")
	res, err = Delete(session, "nonexistingtable", key)
	log.Printf("Result: %v\n", res)

	log.Println("Delete Table")
	res, err = DeleteTable(session, tableName)
	log.Printf("Result: %v\n", res)
}

func TestRun2(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	session, err := Connect("127.0.0.1:8887", "admin", "admin")
	if err != nil {
		log.Println(err)
		return
	}
	defer Disconnect(session)
	tableName := "ctw"
	keyDef := []string{"imsi", "ts"}
	options := map[string]interface{}{
		"type":       "leveldbwrapped",
		"data_model": "map",
		"wrapper": Wrapper{
			NumOfBuckets: 5,
			TimeMargin: TimeMargin{
				Unit:  Minutes,
				Value: 5},
			SizeMargin: SizeMargin{
				Unit:  Megabytes,
				Value: 100},
		},
		"comparator":         "descending",
		"time_series":        false,
		"shards":             8,
		"distributed":        true,
		"replication_factor": 1,
		"hash_exclude":       []string{"ts"},
	}

	log.Println("Create Table")
	res, err := CreateTable(session, tableName, keyDef, options)
	log.Printf("Result: %v\n", res)
	log.Println("Delete Table")
	res, err = DeleteTable(session, tableName)
	log.Printf("Result: %v\n", res)
}

func TestRun3(t *testing.T) {
	log.Println("Testing Concurrent Operations..")
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	session, err := Connect("127.0.0.1:8887", "admin", "admin")
	defer Disconnect(session)
	if err != nil {
		log.Println(err)
		return
	}

	tableName := "ctt"
	keyDef := []string{"ts"}
	options := map[string]interface{}{
		"type":               "leveldb",
		"data_model":         "array",
		"comparator":         "descending",
		"time_series":        false,
		"shards":             8,
		"distributed":        true,
		"replication_factor": 1,
		"hash_exclude":       []string{"ts"},
	}

	res, err := CreateTable(session, tableName, keyDef, options)

	reduce := make(chan bool, 1024)
	defer close(reduce)
	max := 65535
	i := 0
	fail := 0

	for i < max {
		go writeRead(session, tableName, reduce)
		i++
	}
	for i > 0 {
		select {
		case b := <-reduce:
			if b == false {
				fail++
			}
			i--
		}
	}
	log.Printf("All routines (%v) returned. Failed: %v\n", max, fail)
	res, err = DeleteTable(session, tableName)
	log.Printf("Result: %v\n", res)
}

func writeRead(s Session, tableName string, reduce chan bool) {
	time_ := time.Now()
	ts, _ := time_.MarshalBinary()
	key := map[string]interface{}{
		"ts": ts,
	}
	columns := map[string]interface{}{
		"timestamp": ts,
		"binary":    make([]byte, 65535),
	}

	_, err := Write(s, tableName, key, columns)
	if err != nil {
		reduce <- false
		return
	}

	res, err := Read(s, tableName, key)
	if err != nil {
		reduce <- false
		return
	}

	switch res.(type) {
	case map[string]interface{}:
		cols := res.(map[string]interface{})
		resTs := cols["timestamp"].([]byte)
		comp := bytes.Compare(resTs, ts)
		if comp == 0 {
			reduce <- true
			return
		} else {
			reduce <- false
			return
		}
	default:
		reduce <- false
	}
}
