package pundun

import (
	"encoding/binary"
	"errors"
	"github.com/erdemaksu/apollo"
	"github.com/golang/protobuf/proto"
	"log"
)

const (
	timeout = 60
)

type Tda struct {
	NumOfBuckets uint32
	TimeMargin   TimeMargin
	TsField      string
	Precision    int
}

type Wrapper struct {
	NumOfBuckets uint32
	TimeMargin   TimeMargin
	SizeMargin   SizeMargin
}

const (
	Second      = 0
	Millisecond = 1
	Microsecond = 2
	Nanosecond  = 3
)

const (
	Seconds = 0
	Minutes = 1
	Hours   = 2
)

const (
	Megabytes = 0
	Gigabytes = 1
)

type TimeMargin struct {
	Unit  int
	Value uint32
}

type SizeMargin struct {
	Unit  int
	Value uint32
}

const (
	Result = 0
	Error  = 1
)

const (
	OK = 0
)

type KVP struct {
	Key     map[string]interface{}
	Columns map[string]interface{}
}

type KVL struct {
	List         []KVP
	Continuation map[string]interface{}
}

type Iterator struct {
	Kvp KVP
	It  []byte
}

const (
	Increment = 0
	Overwrite = 1
)

const (
	Leveldb           = 0
	MemLeveldb        = 1
	LeveldbWrapped    = 2
	MemLeveldbWrapped = 3
	LeveldbTda        = 4
	MemLeveldbTda     = 5
)

const (
	VirtualNodes = 0
	Consistent   = 1
	Uniform      = 2
	Rendezvous   = 3
)

type UpdateOperation struct {
	Field        string
	Instruction  int
	Value        interface{}
	DefaultValue interface{}
	Threshold    *uint32
	SetValue     *uint32
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
				log.Printf("Stopping id server..\n")
				close(req)
				close(resp)
				break
			}
		}
	}
}

func CreateTable(s Session, tableName string, key []string, options map[string]interface{}) (interface{}, error) {
	tableOptions := fixOptions(options)

	createTable := &apollo.CreateTable{
		TableName:    *proto.String(tableName),
		Keys:         key,
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

func DeleteTable(s Session, tableName string) (interface{}, error) {
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

func OpenTable(s Session, tableName string) (interface{}, error) {
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

func CloseTable(s Session, tableName string) (interface{}, error) {
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

func TableInfo(s Session, tableName string, attrs []string) (interface{}, error) {
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

func Read(s Session, tableName string, key map[string]interface{}) (interface{}, error) {
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

func Write(s Session, tableName string, key, columns map[string]interface{}) (interface{}, error) {
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

func Update(s Session, tableName string, key map[string]interface{}, upOps []UpdateOperation) (interface{}, error) {
	keyFields := fixFields(key)
	updateOperations := fixUpdateOperations(upOps)
	update := &apollo.Update{
		TableName:       *proto.String(tableName),
		Key:             keyFields,
		UpdateOperation: updateOperations,
	}

	procedure := &apollo.ApolloPdu_Update{
		Update: update,
	}

	pdu := &apollo.ApolloPdu{
		Procedure: procedure,
	}

	res, err := run_transaction(s, pdu)
	return res, err
}

func Delete(s Session, tableName string, key map[string]interface{}) (interface{}, error) {
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

func ReadRange(s Session, tableName string, skey, ekey map[string]interface{}, limit int) (interface{}, error) {
	skeyFields := fixFields(skey)
	ekeyFields := fixFields(ekey)
	readRange := &apollo.ReadRange{
		TableName: *proto.String(tableName),
		StartKey:  skeyFields,
		EndKey:    ekeyFields,
		Limit:     *proto.Uint32(uint32(limit)),
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

func ReadRangeN(s Session, tableName string, skey map[string]interface{}, n int) (interface{}, error) {
	skeyFields := fixFields(skey)
	readRangeN := &apollo.ReadRangeN{
		TableName: *proto.String(tableName),
		StartKey:  skeyFields,
		N:         *proto.Uint32(uint32(n)),
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

func First(s Session, tableName string) (interface{}, error) {
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

func Last(s Session, tableName string) (interface{}, error) {
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

func Seek(s Session, tableName string, key map[string]interface{}) (interface{}, error) {
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

func Next(s Session, it []byte) (interface{}, error) {
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

func Prev(s Session, it []byte) (interface{}, error) {
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

func run_transaction(s Session, pdu *apollo.ApolloPdu) (interface{}, error) {
	tid := GetTid(s)
	pdu = make_pdu(pdu, tid)
	pduBin, err := proto.Marshal(pdu)
	if err != nil {
		log.Println("marshaling error: ", err)
		return nil, err
	}

	recv, err := send(s, pduBin)

	if err != nil {
		log.Println("error: ", err)
	}
	res, err := waitForResponse(recv)
	return res, err
}

func make_pdu(pdu *apollo.ApolloPdu, tid uint32) *apollo.ApolloPdu {
	version := &apollo.Version{
		Major: *proto.Uint32(0),
		Minor: *proto.Uint32(1),
	}
	pdu.Version = version
	pdu.TransactionId = *proto.Uint32(tid)
	return pdu
}

func send(s Session, data []byte) ([]byte, error) {
	response := SendMsg(s, data)
	return response, nil
}

func waitForResponse(recv []byte) (interface{}, error) {
	recvPdu := &apollo.ApolloPdu{}
	err := proto.Unmarshal(recv, recvPdu)

	if err != nil {
		log.Println("unmarshaling error: ", err)
		return nil, err
	}

	e := recvPdu.GetError()
	r := recvPdu.GetResponse()
	if r != nil {
		result := getResult(r)
		return result, err
	}
	if e != nil {
		pErr := getError(e)
		return pErr, err
	}
	return nil, errors.New("invalid response")
}

func getResult(r *apollo.Response) interface{} {
	ok := r.GetOk()
	if ok != "" {
		return OK
	}
	c := r.GetColumns()
	if c != nil {
		return formatColumns(c)
	}
	kcp := r.GetKeyColumnsPair()
	if kcp != nil {
		return formatKcp(kcp)
	}
	kcl := r.GetKeyColumnsList()
	if kcl != nil {
		return formatKcl(kcl)
	}
	pl := r.GetProplist()
	if pl != nil {
		return formatColumns(pl)
	}
	it := r.GetKcpIt()
	if it != nil {
		return formatKcpIt(it)
	} else {
		return nil
	}
}

func getError(e *apollo.Error) map[string]string {
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

func formatKcp(kcp *apollo.KeyColumnsPair) KVP {
	keyFields := kcp.GetKey()
	columnFields := kcp.GetColumns()
	keyMap := formatFields(keyFields)
	columnsMap := formatFields(columnFields)
	return KVP{keyMap, columnsMap}
}

func formatKcpIt(kcpIt *apollo.KcpIt) Iterator {
	kvp := kcpIt.GetKeyColumnsPair()
	it := kcpIt.It
	return Iterator{formatKcp(kvp), it}
}

func formatKcl(kcl *apollo.KeyColumnsList) KVL {
	protoList := kcl.GetList()
	list := make([]KVP, len(protoList))
	for i := range protoList {
		list[i] = formatKcp(protoList[i])
	}
	cont := kcl.GetContinuation()
	contKey := cont.GetKey()
	return KVL{list, formatFields(contKey)}
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
			m[f.Name] = int64(f.GetInt())
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
		case MemLeveldb:
			tableType = apollo.Type_MEMLEVELDB
		case LeveldbWrapped:
			tableType = apollo.Type_LEVELDBWRAPPED
		case MemLeveldbWrapped:
			tableType = apollo.Type_MEMLEVELDBWRAPPED
		case LeveldbTda:
			tableType = apollo.Type_LEVELDBTDA
		case MemLeveldbTda:
			tableType = apollo.Type_MEMLEVELDBTDA
		default:
		}
		opt = &apollo.TableOption{&apollo.TableOption_Type{tableType}}
	case "data_model":
		dataModel := apollo.DataModel_ARRAY
		switch v {
		case "kv":
			dataModel = apollo.DataModel_KV
		case "map":
			dataModel = apollo.DataModel_MAP
		default:
		}
		opt = &apollo.TableOption{&apollo.TableOption_DataModel{dataModel}}
	case "wrapper":
		wrapper := fixWrapper(v.(Wrapper))
		opt = &apollo.TableOption{&apollo.TableOption_Wrapper{wrapper}}
	case "tda":
		tda := fixTda(v.(Tda))
		opt = &apollo.TableOption{&apollo.TableOption_Tda{tda}}
	case "hashing_method":
		hm := apollo.HashingMethod_UNIFORM
		switch v {
		case VirtualNodes:
			hm = apollo.HashingMethod_VIRTUALNODES
		case Consistent:
			hm = apollo.HashingMethod_CONSISTENT
		case Uniform:
			hm = apollo.HashingMethod_UNIFORM
		case Rendezvous:
			hm = apollo.HashingMethod_RENDEZVOUS
		default:
		}
		opt = &apollo.TableOption{&apollo.TableOption_HashingMethod{hm}}
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

func fixTda(t Tda) *apollo.Tda {
	tm := &t.TimeMargin
	tda := apollo.Tda{
		NumOfBuckets: t.NumOfBuckets,
		TsField:      t.TsField,
	}
	fixTdaMargin(&tda, tm)
	fixPrecision(&tda, t.Precision)
	return &tda
}

func fixPrecision(tda *apollo.Tda, p int) {
	precision := apollo.TimeUnit_SECOND
	switch p {
	case Millisecond:
		precision = apollo.TimeUnit_MILLISECOND
	case Microsecond:
		precision = apollo.TimeUnit_MICROSECOND
	case Nanosecond:
		precision = apollo.TimeUnit_NANOSECOND
	default:
	}
	tda.Precision = precision
}

func fixTdaMargin(tda *apollo.Tda, tm *TimeMargin) {
	if tm != nil {
		timeUnit := tm.Unit
		t := tm.Value
		switch timeUnit {
		case Seconds:
			tda.TimeMargin = &apollo.Tda_Seconds{*proto.Uint32(t)}
		case Minutes:
			tda.TimeMargin = &apollo.Tda_Minutes{*proto.Uint32(t)}
		case Hours:
			tda.TimeMargin = &apollo.Tda_Hours{*proto.Uint32(t)}
		}
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
			w.TimeMargin = &apollo.Wrapper_Seconds{*proto.Uint32(t)}
		case Minutes:
			w.TimeMargin = &apollo.Wrapper_Minutes{*proto.Uint32(t)}
		case Hours:
			w.TimeMargin = &apollo.Wrapper_Hours{*proto.Uint32(t)}
		}
	}
}

func fixSizeMargin(w *apollo.Wrapper, sm *SizeMargin) {
	if sm != nil {
		sizeUnit := sm.Unit
		s := sm.Value
		switch sizeUnit {
		case Megabytes:
			w.SizeMargin = &apollo.Wrapper_Megabytes{*proto.Uint32(s)}
		case Gigabytes:
			w.SizeMargin = &apollo.Wrapper_Gigabytes{*proto.Uint32(s)}
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
			&apollo.Field_Int{*proto.Int64(int64(v.(int)))},
		}
	case int8:
		field = &apollo.Field{*proto.String(k),
			&apollo.Field_Int{*proto.Int64(int64(v.(int8)))},
		}
	case int16:
		field = &apollo.Field{*proto.String(k),
			&apollo.Field_Int{*proto.Int64(int64(v.(int16)))},
		}
	case int32:
		field = &apollo.Field{*proto.String(k),
			&apollo.Field_Int{*proto.Int64(int64(v.(int32)))},
		}
	case int64:
		field = &apollo.Field{*proto.String(k),
			&apollo.Field_Int{*proto.Int64(v.(int64))},
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

func fixUpdateOperations(upOps []UpdateOperation) []*apollo.UpdateOperation {
	updateOperations := make([]*apollo.UpdateOperation, 0)
	for i := range upOps {
		updateOperations = fixUpdateOperation(upOps[i], updateOperations)
	}
	return updateOperations
}

func fixUpdateOperation(upOp UpdateOperation, updateOperations []*apollo.UpdateOperation) []*apollo.UpdateOperation {
	i := upOp.Instruction
	instruction := apollo.UpdateInstruction_INCREMENT
	switch i {
	case Overwrite:
		instruction = apollo.UpdateInstruction_OVERWRITE
	default:
	}
	var updateInstruction *apollo.UpdateInstruction
	threshold := encodeInt32(upOp.Threshold)
	setvalue := encodeInt32(upOp.SetValue)
	updateInstruction = &apollo.UpdateInstruction{
		instruction,
		threshold,
		setvalue}

	value := fixValue(upOp.Value)
	defaultValue := fixDefaultValue(upOp.DefaultValue)

	var updateOperation *apollo.UpdateOperation
	updateOperation = &apollo.UpdateOperation{
		*proto.String(upOp.Field),
		updateInstruction,
		value,
		defaultValue}

	len := len(updateOperations) + 1
	newUpdateOperations := make([]*apollo.UpdateOperation, len)
	copy(newUpdateOperations, updateOperations[:])
	newUpdateOperations[len-1] = updateOperation
	return newUpdateOperations
}

func fixValue(v interface{}) *apollo.Value {
	var value *apollo.Value
	switch v.(type) {
	case string:
		value = &apollo.Value{&apollo.Value_String_{*proto.String(v.(string))}}
	case []byte:
		value = &apollo.Value{&apollo.Value_Binary{v.([]byte)}}
	case int:
		value = &apollo.Value{&apollo.Value_Int{*proto.Int64(int64(v.(int)))}}
	case int8:
		value = &apollo.Value{&apollo.Value_Int{*proto.Int64(int64(v.(int8)))}}
	case int16:
		value = &apollo.Value{&apollo.Value_Int{*proto.Int64(int64(v.(int16)))}}
	case int32:
		value = &apollo.Value{&apollo.Value_Int{*proto.Int64(int64(v.(int32)))}}
	case int64:
		value = &apollo.Value{&apollo.Value_Int{*proto.Int64(v.(int64))}}
	case float64:
		value = &apollo.Value{&apollo.Value_Double{*proto.Float64(v.(float64))}}
	case bool:
		value = &apollo.Value{&apollo.Value_Boolean{*proto.Bool(v.(bool))}}
	default:
		if v == nil {
			value = &apollo.Value{&apollo.Value_Null{
				[]byte{}},
			}
		}
	}
	return value
}

func fixDefaultValue(v interface{}) *apollo.Value {
	if v == nil {
		return nil
	} else {
		return fixValue(v)
	}
}

func encodeInt32(v *uint32) []byte {
	if v != nil {
		bytes := make([]byte, 4)
		binary.BigEndian.PutUint32(bytes, *v)
		return bytes
	} else {
		return make([]byte, 0)
	}
}
