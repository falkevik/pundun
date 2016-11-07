package pundun

import (
	"crypto/tls"
	"encoding/binary"
	"github.com/erdemaksu/scram"
	"log"
	"net"
	"time"
)

const (
	stop = 0
)

type Session struct {
	manChan  chan int
	sendChan chan Client
	tidChan  chan uint16
}

type Client struct {
	data []byte
	ch   chan []byte
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

	manChan := make(chan int, 1024)
	sendChan := make(chan Client, 1024)
	recvChan := make(chan []byte, 1024)

	go serverLoop(conn, manChan, sendChan, recvChan)
	go recvLoop(conn, recvChan)

	tidChan := make(chan uint16, 1)
	var tid uint16 = 0
	go tidServer(tid, tidChan)

	return Session{manChan, sendChan, tidChan}, err
}

func Disconnect(s Session) {
	defer close(s.manChan)
	s.manChan <- stop
}

func GetTid(s Session) int32 {
	tid := <-s.tidChan
	return int32(tid)
}

func SendMsg(s Session, data []byte) []byte {
	ch := make(chan []byte)
	s.sendChan <- Client{data, ch}
	select {
	case pdu := <-ch:
		close(ch)
		return pdu
	case <-time.After(30 * time.Second):
		close(ch)
		log.Println("timed out")
		return []byte{}
	}
}

func serverLoop(conn net.Conn, manChan chan int, sendChan chan Client, recvChan chan []byte) {
	var cid uint16 = 65535
	clients := make(map[uint16]Client)
	defer close(sendChan)
	defer conn.Close()
	for {
		select {
		case msg := <-manChan:
			switch msg {
			case stop:
				log.Println("Stopping session..")
				return
			}
		case data := <-recvChan:
			len := len(data)
			corrIdBytes := make([]byte, 2)
			pduBytes := make([]byte, len-2)
			copy(corrIdBytes, data[:2])
			copy(pduBytes, data[2:])
			corrId := binary.BigEndian.Uint16(corrIdBytes)
			client := clients[corrId]
			client.ch <- pduBytes
			delete(clients, corrId)
		case client := <-sendChan:
			if checkCorrId(clients, cid) {
				corrId := make([]byte, 2)
				binary.BigEndian.PutUint16(corrId, cid)
				data := client.data
				len := uint32(len(data))
				wire := make([]byte, len+6)
				binary.BigEndian.PutUint32(wire, len+2)
				copy(wire[4:], corrId)
				copy(wire[6:], data)
				clients[cid] = Client{[]byte{}, client.ch}
				conn.Write(wire)
				cid++
			} else {
				client.ch <- []byte{}
			}
		}
	}
}

func checkCorrId(clients map[uint16]Client, corrId uint16) bool {
	if oldClient, ok := clients[corrId]; ok {
		if _, open := <-oldClient.ch; open {
			return false
		} else {
			delete(clients, corrId)
			return true
		}

	} else {
		return true
	}
}

func recvLoop(conn net.Conn, recvChan chan []byte) {
	defer close(recvChan)
	for {
		// Receive length of package
		lenBuf := make([]byte, 4)
		n, err := conn.Read(lenBuf)

		if n != 4 || err != nil {
			log.Printf("n: %v, error: %v", n, err)
			return
		} else {
			// Receive encoded pdu
			len := binary.BigEndian.Uint32(lenBuf)
			buf := make([]byte, len)
			n, err = conn.Read(buf)
			if uint32(n) != len || err != nil {
				log.Println("error: ", err)
			} else {
				recvChan <- buf
			}
		}
	}
}

func tidServer(tid uint16, tidChan chan uint16) {
	for {
		tidChan <- tid
		tid++
	}
}
