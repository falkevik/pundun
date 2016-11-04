package pundun

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"github.com/erdemaksu/apollo"
	"github.com/erdemaksu/scram"
	"github.com/golang/protobuf/proto"
	"log"
	"net"
	"time"
)

const (
	timeout = 60
)

type Session struct {
	conn    net.Conn
	tidReq  chan string
	tidResp chan int32
	timeout time.Duration
}

type Wrapper struct {
	NumOfBuckets int32
	TimeMargin   TimeMargin
	SizeMargin   SizeMargin
}

const (
	Seconds = 0
	Minutes = 1
	Hours   = 2
)

const (
	Megabytes = 0
)

type TimeMargin struct {
	Unit  int
	Value int32
}

type SizeMargin struct {
	Unit  int
	Value int32
}

const (
	Result = 0
	Error  = 1
)

type Response struct {
	Type  int
	Value interface{}
}

func tidServe(tid, max int32, req chan string, resp chan int32) {
	for {
		select {
		case msg := <-req:
			switch msg {
			case "tid":
				resp <- tid
				if tid == max {
					tid = 0
				} else {
					tid++
				}
			case "stop":
				close(req)
				close(resp)
				break
			}
		}
	}
}

func Connect(host, user, pass string) (Session, error) {
	conf := &tls.Config{
		InsecureSkipVerify: true,
	}

	conn, err := tls.Dial("tcp", host, conf)
	if err != nil {
		log.Println(err)
		return Session{}, err
	}

	authErr := scram.Authenticate(conn, user, pass)
	if authErr != nil {
		log.Println(authErr)
		conn.Close()
		return Session{}, authErr
	}
	log.Println("Connected to pundun node.")

	tidReq := make(chan string)
	tidResp := make(chan int32)
	go tidServe(0, 65535, tidReq, tidResp)
	return Session{conn, tidReq, tidResp, timeout}, err
}

func Disconnect(s Session) {
	s.tidReq <- "stop"
	conn := s.conn
	conn.Close()
}

func CreateTable(s Session, tableName string, key, columns, indexes []string, options map[string]interface{}) (Response, error) {
	tableOptions := fixOptions(options)

	createTable := &apollo.CreateTable{
		TableName:    *proto.String(tableName),
		Keys:         key,
		Columns:      columns,
		Indexes:      indexes,
		TableOptions: tableOptions,
	}

	procedure := &apollo.ApolloPdu_CreateTable{
		CreateTable: createTable,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func DeleteTable(s Session, tableName string) (Response, error) {
	deleteTable := &apollo.DeleteTable{
		TableName: *proto.String(tableName),
	}

	procedure := &apollo.ApolloPdu_DeleteTable{
		DeleteTable: deleteTable,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}
	res, err := run_transaction(s, pdu)
	return res, err
}

func OpenTable(s Session, tableName string) (Response, error) {
	openTable := &apollo.OpenTable{
		TableName: *proto.String(tableName),
	}

	procedure := &apollo.ApolloPdu_OpenTable{
		OpenTable: openTable,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func CloseTable(s Session, tableName string) (Response, error) {
	closeTable := &apollo.CloseTable{
		TableName: *proto.String(tableName),
	}

	procedure := &apollo.ApolloPdu_CloseTable{
		CloseTable: closeTable,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func TableInfo(s Session, tableName string, attrs []string) (Response, error) {
	attributes := fixAttributes(attrs)
	tableInfo := &apollo.TableInfo{
		TableName:  *proto.String(tableName),
		Attributes: attributes,
	}

	procedure := &apollo.ApolloPdu_TableInfo{
		TableInfo: tableInfo,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func Read(s Session, tableName string, key map[string]interface{}) (Response, error) {
	keyFields := fixFields(key)
	read := &apollo.Read{
		TableName: *proto.String(tableName),
		Key:       keyFields,
	}

	procedure := &apollo.ApolloPdu_Read{
		Read: read,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func Write(s Session, tableName string, key, columns map[string]interface{}) (Response, error) {
	keyFields := fixFields(key)
	columnFields := fixFields(columns)
	write := &apollo.Write{
		TableName: *proto.String(tableName),
		Key:       keyFields,
		Columns:   columnFields,
	}

	procedure := &apollo.ApolloPdu_Write{
		Write: write,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func Delete(s Session, tableName string, key map[string]interface{}) (Response, error) {
	keyFields := fixFields(key)
	delete := &apollo.Delete{
		TableName: *proto.String(tableName),
		Key:       keyFields,
	}

	procedure := &apollo.ApolloPdu_Delete{
		Delete: delete,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func ReadRange(s Session, tableName string, skey, ekey map[string]interface{}, limit int) (Response, error) {
	skeyFields := fixFields(skey)
	ekeyFields := fixFields(ekey)
	readRange := &apollo.ReadRange{
		TableName: *proto.String(tableName),
		StartKey:  skeyFields,
		EndKey:    ekeyFields,
		Limit:     *proto.Int32(int32(limit)),
	}

	procedure := &apollo.ApolloPdu_ReadRange{
		ReadRange: readRange,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func ReadRangeN(s Session, tableName string, skey map[string]interface{}, n int) (Response, error) {
	skeyFields := fixFields(skey)
	readRangeN := &apollo.ReadRangeN{
		TableName: *proto.String(tableName),
		StartKey:  skeyFields,
		N:         *proto.Int32(int32(n)),
	}

	procedure := &apollo.ApolloPdu_ReadRangeN{
		ReadRangeN: readRangeN,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func First(s Session, tableName string) (Response, error) {
	first := &apollo.First{
		TableName: *proto.String(tableName),
	}

	procedure := &apollo.ApolloPdu_First{
		First: first,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func Last(s Session, tableName string) (Response, error) {
	last := &apollo.Last{
		TableName: *proto.String(tableName),
	}

	procedure := &apollo.ApolloPdu_Last{
		Last: last,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func Seek(s Session, tableName string, key map[string]interface{}) (Response, error) {
	keyFields := fixFields(key)
	seek := &apollo.Seek{
		TableName: *proto.String(tableName),
		Key:       keyFields,
	}

	procedure := &apollo.ApolloPdu_Seek{
		Seek: seek,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func Next(s Session, it []byte) (Response, error) {
	next := &apollo.Next{
		It: it,
	}

	procedure := &apollo.ApolloPdu_Next{
		Next: next,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func Prev(s Session, it []byte) (Response, error) {
	prev := &apollo.Prev{
		It: it,
	}

	procedure := &apollo.ApolloPdu_Prev{
		Prev: prev,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func run_transaction(s Session, pdu *apollo.ApolloPdu) (Response, error) {
	s.tidReq <- "tid"
	tid := <-s.tidResp
	pdu = make_pdu(pdu, tid)
	data, err := proto.Marshal(pdu)
	if err != nil {
		log.Println("marshaling error: ", err)
		var res = Response{}
		return res, err
	}

	_, err = send(s, data)
	if err != nil {
		log.Println("error: ", err)
	}
	res, err := waitForResponse(s)
	return res, err
}

func make_pdu(pdu *apollo.ApolloPdu, tid int32) *apollo.ApolloPdu {
	version := &apollo.Version{
		Major: *proto.Int32(0),
		Minor: *proto.Int32(1),
	}
	pdu.Version = version
	pdu.TransactionId = *proto.Int32(tid)
	return pdu
}

func send(s Session, data []byte) (int, error) {
	len := uint32(len(data))
	wire := make([]byte, len+4)
	binary.BigEndian.PutUint32(wire, len)
	copy(wire[4:], data[:])
	return s.conn.Write(wire)
}

func waitForResponse(s Session) (Response, error) {
	t := time.Now().Add(s.timeout * time.Second)
	s.conn.SetDeadline(t)

	recv := make([]byte, 4)
	n, err := s.conn.Read(recv)

	if n != 4 || err != nil {
		log.Printf("n: %v, error: %v", n, err)
		return Response{}, err
	}

	len := binary.BigEndian.Uint32(recv)
	recv = make([]byte, len)

	n, err = s.conn.Read(recv)
	if err != nil {
		log.Println("error: ", err)
		return Response{}, err
	}

	recvPdu := &apollo.ApolloPdu{}
	err = proto.Unmarshal(recv, recvPdu)

	if err != nil {
		log.Println("unmarshaling error: ", err)
		return Response{}, err
	}

	log.Println(recvPdu)

	e := recvPdu.GetError()
	r := recvPdu.GetResponse()
	if r != nil {
		rmap := getResultMap(r)
		return Response{Result, rmap}, err
	}
	if e != nil {
		emap := getErrorMap(e)
		return Response{Error, emap}, err
	}
	return Response{}, errors.New("invalid response")
}

func getResultMap(r *apollo.Response) map[string]interface{} {
	m := make(map[string]interface{})
	ok := r.GetOk()
	if ok != "" {
		m["ok"] = "ok"
	}
	c := r.GetColumns()
	if c != nil {
		m["columns"] = formatColumns(c)
	}
	kcp := r.GetKeyColumnsPair()
	if kcp != nil {
		m["keyColumnsPair"] = formatKcp(kcp)
	}
	kcl := r.GetKeyColumnsList()
	if kcl != nil {
		m["keyColumnsList"] = formatKcl(kcl)
	}
	pl := r.GetProplist()
	if pl != nil {
		m["proplist"] = formatColumns(pl)
	}
	it := r.GetKcpIt()
	if it != nil {
		m["kcpIt"] = formatKcpIt(it)
	}
	return m
}

func getErrorMap(e *apollo.Error) map[string]string {
	m := make(map[string]string)
	t := e.GetTransport()
	if t != "" {
		m["transport"] = t
	}
	p := e.GetProtocol()
	if p != "" {
		m["protocol"] = p
	}
	s := e.GetSystem()
	if s != "" {
		m["system"] = s
	}
	o := e.GetMisc()
	if o != "" {
		m["misc"] = o
	}
	return m
}

func formatColumns(c *apollo.Fields) map[string]interface{} {
	fields := c.GetFields()
	return formatFields(fields)
}

func formatKcp(kcp *apollo.KeyColumnsPair) map[string]interface{} {
	keyFields := kcp.GetKey()
	columnFields := kcp.GetColumns()
	keyMap := formatFields(keyFields)
	columnsMap := formatFields(columnFields)
	m := make(map[string]interface{})
	m["key"] = keyMap
	m["columns"] = columnsMap
	return m
}

func formatKcpIt(kcpIt *apollo.KcpIt) map[string]interface{} {
	kcp := kcpIt.GetKeyColumnsPair()
	it := kcpIt.It

	m := make(map[string]interface{})
	m["kcp"] = formatKcp(kcp)
	m["it"] = it

	return m
}

func formatKcl(kcl *apollo.KeyColumnsList) map[string]interface{} {
	list := kcl.GetList()
	cont := kcl.GetContinuation()
	array := make([]map[string]interface{}, len(list))
	for i := range list {
		array[i] = formatKcp(list[i])
	}

	m := make(map[string]interface{})
	m["list"] = array

	contKey := cont.GetKey()
	if contKey == nil {
		m["continuation"] = "complete"
	} else {
		key := formatFields(contKey)
		m["continuation"] = key
	}
	return m
}

func formatFields(fields []*apollo.Field) map[string]interface{} {
	m := make(map[string]interface{})
	for i := range fields {
		f := fields[i]
		switch f.GetValue().(type) {
		case *apollo.Field_String_:
			m[f.Name] = f.GetString_()
		case *apollo.Field_Binary:
			m[f.Name] = f.GetBinary()
		case *apollo.Field_Int:
			m[f.Name] = int(f.GetInt())
		case *apollo.Field_Double:
			m[f.Name] = f.GetDouble()
		case *apollo.Field_Boolean:
			m[f.Name] = f.GetBoolean()
		case *apollo.Field_Null:
			m[f.Name] = f.GetNull()
		}
	}
	return m
}

func fixOptions(options map[string]interface{}) []*apollo.TableOption {
	tableOptions := make([]*apollo.TableOption, 0)
	for k, v := range options {
		tableOptions = fixOption(k, v, tableOptions)
	}
	return tableOptions
}

func fixOption(k string, v interface{}, opts []*apollo.TableOption) []*apollo.TableOption {
	var opt *apollo.TableOption

	switch k {
	case "type":
		tableType := apollo.Type_LEVELDB
		switch v {
		case "memleveldb":
			tableType = apollo.Type_MEMLEVELDB
		case "leveldbwrapped":
			tableType = apollo.Type_LEVELDBWRAPPED
		case "memleveldbwrapped":
			tableType = apollo.Type_MEMLEVELDBWRAPPED
		default:
		}
		opt = &apollo.TableOption{&apollo.TableOption_Type{tableType}}
	case "data_model":
		dataModel := apollo.DataModel_BINARY
		switch v {
		case "array":
			dataModel = apollo.DataModel_ARRAY
		case "leveldbwrapped":
			dataModel = apollo.DataModel_HASH
		default:
		}
		opt = &apollo.TableOption{&apollo.TableOption_DataModel{dataModel}}
	case "wrapper":
		wrapper := fixWrapper(v.(Wrapper))
		opt = &apollo.TableOption{&apollo.TableOption_Wrapper{wrapper}}
	default:
	}
	if opt != nil {
		len := len(opts) + 1
		newOpts := make([]*apollo.TableOption, len)
		copy(newOpts, opts[:])
		newOpts[len-1] = opt
		return newOpts
	} else {
		return opts
	}
}

func fixWrapper(w Wrapper) *apollo.Wrapper {
	tm := &w.TimeMargin
	sm := &w.SizeMargin
	wrapper := apollo.Wrapper{
		NumOfBuckets: w.NumOfBuckets,
	}
	fixTimeMargin(&wrapper, tm)
	fixSizeMargin(&wrapper, sm)

	return &wrapper
}

func fixTimeMargin(w *apollo.Wrapper, tm *TimeMargin) {
	if tm != nil {
		timeUnit := tm.Unit
		t := tm.Value
		switch timeUnit {
		case Seconds:
			w.TimeMargin = &apollo.Wrapper_Seconds{*proto.Int32(t)}
		case Minutes:
			w.TimeMargin = &apollo.Wrapper_Minutes{*proto.Int32(t)}
		case Hours:
			w.TimeMargin = &apollo.Wrapper_Hours{*proto.Int32(t)}
		}
	}
}

func fixSizeMargin(w *apollo.Wrapper, sm *SizeMargin) {
	if sm != nil {
		sizeUnit := sm.Unit
		s := sm.Value
		switch sizeUnit {
		case Megabytes:
			w.SizeMargin = &apollo.Wrapper_Megabytes{*proto.Int32(s)}
		}
	}
}

func fixAttributes(attrs []string) []string {
	ps := make([]string, len(attrs))
	for a := range attrs {
		ps[a] = *proto.String(attrs[a])
	}
	return ps
}

func fixFields(m map[string]interface{}) []*apollo.Field {
	fields := make([]*apollo.Field, 0)
	for k, v := range m {
		fields = fixField(k, v, fields)
	}
	return fields
}

func fixField(k string, v interface{}, fields []*apollo.Field) []*apollo.Field {
	var field *apollo.Field

	switch v.(type) {
	case string:
		field = &apollo.Field{*proto.String(k),
			&apollo.Field_String_{*proto.String(v.(string))},
		}
	case []byte:
		field = &apollo.Field{*proto.String(k),
			&apollo.Field_Binary{v.([]byte)},
		}
	case int:
		field = &apollo.Field{*proto.String(k),
			&apollo.Field_Int{*proto.Int32(int32(v.(int)))},
		}
	case float64:
		field = &apollo.Field{*proto.String(k),
			&apollo.Field_Double{*proto.Float64(v.(float64))},
		}
	case bool:
		field = &apollo.Field{*proto.String(k),
			&apollo.Field_Boolean{*proto.Bool(v.(bool))},
		}
	default:
		if v == nil {
			field = &apollo.Field{*proto.String(k),
				&apollo.Field_Null{
					[]byte{},
				},
			}
		}
	}
	len := len(fields) + 1
	newFields := make([]*apollo.Field, len)
	copy(newFields, fields[:])
	newFields[len-1] = field
	return newFields
}
