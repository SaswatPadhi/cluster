package cluster

import (
	zmq "github.com/pebbe/zmq4"

	"encoding/gob"
	"encoding/json"
	"io/ioutil"

	"bytes"
	"fmt"
	"io"
	"log"
	"sync"
)

const (
	// Error messages
	INVALID_SELF_ID = "Server's pid missing in the config file."
	INVALID_PEER_ID = "Target pid missing in the config file."
	UNMARSHAL_ERROR = "Could not unmarshal incoming envelope."
)

// Peer holds an id and an associated address for another server in the cluster.
type Peer struct {
	Pid  int
	Addr string
	sock *zmq.Socket
}

// Server is an object that represents a single server in the cluster.
type Server struct {
	pid            int
	addr           string
	sock           *zmq.Socket
	peers          map[int]Peer
	inbox          chan *Envelope
	outbox         chan *Envelope
	stop           chan bool
	stopped        sync.WaitGroup
	terminate_code []byte
}

// NewServer creates and returns a new Server object with the provided id and
// the peers provided in the config file.
func NewServer(id int, config string) (s *Server, err error) {
	DBG_INFO.Println(fmt.Sprintf("Creating server [id: %d, config: %s]", id, config))
	defer DBG_INFO.Println(fmt.Sprintf("Server creation returns [err: %s]", err))

	s = &Server{}
	s.pid = id
	s.peers = make(map[int]Peer)

	s.stop = make(chan bool, 2)
	s.inbox = make(chan *Envelope, 128)
	s.outbox = make(chan *Envelope, 128)

	data, err := ioutil.ReadFile(config)
	if err != nil {
		DBG_EROR.Println(fmt.Sprintf("Error reading %s [err: %s]", config, err))
		return
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var p Peer
		if err = decoder.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			DBG_EROR.Println(fmt.Sprintf("Error parsing json [err: %s]", err))
			return
		} else if p.Pid == s.pid {
			s.addr = p.Addr
			continue
		}
		p.sock = nil
		s.peers[p.Pid] = p
	}

	if len(s.addr) == 0 {
		err = fmt.Errorf(INVALID_SELF_ID)
		DBG_EROR.Println(fmt.Sprintf("Error creating server [err: %s]", err))
		return
	}

	if s.terminate_code, err = GenerateUUID(); err != nil {
		DBG_EROR.Println(fmt.Sprintf("Error generating UUID [err: %s]", err))
		return
	}

	if s.sock, err = zmq.NewSocket(zmq.PULL); err != nil {
		DBG_EROR.Println(fmt.Sprintf("Error creation ZMQ socket [err: %s]", err))
		return
	}

	s.stopped.Add(1)
	go s.monitorInbox()
	go s.monitorOutbox()

	err = s.sock.Bind("tcp://" + s.addr)
	if err != nil {
		DBG_EROR.Println(fmt.Sprintf("Error binding socket [err: %s]", err))
	}
	return
}

// Sends an envelope to the peer with the provided peer id.
func (s *Server) writeToServer(pid int, env *Envelope) (err error) {
	DBG_INFO.Println(fmt.Sprintf("Sending envelope [from: %d, id: %d, env: %s]", s.pid, pid, env.toString()))
	defer DBG_INFO.Println(fmt.Sprintf("Sending envelope returns [err: %s]", err))

	p, ok := s.peers[pid]
	if !ok {
		DBG_WARN.Println(fmt.Sprintf("Error sending envelope %s [err: Invalid Peer ID]", env.toString()))
		err = fmt.Errorf("%s -- %s", INVALID_PEER_ID, pid)
		return
	}

	env.Pid = s.pid
	msg := new(bytes.Buffer)
	err = gob.NewEncoder(msg).Encode(env)
	if err != nil {
		DBG_WARN.Println(fmt.Sprintf("Error encoding envelope %s [err: %s]", env, err))
		return
	}

	if p.sock == nil {
		if p.sock, err = zmq.NewSocket(zmq.PUSH); err != nil {
			DBG_EROR.Println(fmt.Sprintf("Error creating ZMQ socket to send %s [err: %s]", env, err))
			return
		}
		p.sock.Connect("tcp://" + p.Addr)
	}
	p.sock.SendBytes(msg.Bytes(), 0)

	return
}

// Reads data from the server's socket and sends them to the provided channel.
func (s *Server) readFromServer(dchan chan []byte) (err error) {
	DBG_INFO.Println(fmt.Sprintf("Reading server channel [id: %d]", s.pid))
	defer DBG_INFO.Println(fmt.Sprintf("Reading server channel returns [err: %s]", err))

	for {
		if reply, err := s.sock.RecvBytes(0); err != nil {
			return err
		} else if bytes.Equal(reply, s.terminate_code) {
			err := s.sock.Close()
			s.stopped.Done()
			return err
		} else {
			dchan <- reply
		}
	}
}

// Monitors the server's inbox channel for envelopes.
func (s *Server) monitorInbox() {
	data_chan := make(chan []byte)
	go s.readFromServer(data_chan)

	for {
		select {
		case <-s.stop:
			return
		case data := <-data_chan:
			var envelope Envelope
			msg := bytes.NewBuffer(data)
			dec := gob.NewDecoder(msg)
			if err := dec.Decode(&envelope); err != nil {
				log.Printf("%s -- %s.", UNMARSHAL_ERROR, data)
			} else {
				s.inbox <- &envelope
			}
		}
	}
}

// Monitors the server's outbox channel for envelopes.
func (s *Server) monitorOutbox() {
	for {
		select {
		case <-s.stop:
			return
		case envelope := <-s.outbox:
			if envelope.Pid != BROADCAST {
				go s.writeToServer(envelope.Pid, envelope)
			} else {
				for p := range s.peers {
					go s.writeToServer(p, envelope)
				}
			}
		}
	}
}

// Stop function attempts to bring down an active server gracefully.
func (s *Server) Stop() error {
	s.stop <- true // for monitorInbox
	s.stop <- true // for monitorOutbox

	sock, err := zmq.NewSocket(zmq.PUSH)
	if err != nil {
		return err
	}
	if err := sock.Connect("tcp://" + s.addr); err != nil {
		return err
	}

	sock.SendBytes(s.terminate_code, 0)
	for _, p := range s.peers {
		if p.sock != nil {
			p.sock.Close()
		}
	}

	s.stopped.Wait()
	return nil
}

// Returns the Pid of the server in the cluster.
func (s *Server) Pid() int {
	return s.pid
}

// Returns the Pid's of all other servers in the same cluster as this server.
func (s *Server) Peers() []int {
	peers := make([]int, 0, len(s.peers))
	for k := range s.peers {
		peers = append(peers, k)
	}
	return peers
}

// Returns the inbox channel of the server.
func (s *Server) Inbox() <-chan *Envelope {
	return s.inbox
}

// Returns the outbox channel of the server.
func (s *Server) Outbox() chan<- *Envelope {
	return s.outbox
}
