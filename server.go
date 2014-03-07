package cluster

import (
	zmq "github.com/pebbe/zmq4"

	"encoding/gob"
	"encoding/json"
	"io/ioutil"

	"bytes"
	"fmt"
	"io"
)

const (
	// Error messages
	INVALID_SELF_ID = "Server's pid missing in the config file."
	INVALID_PEER_ID = "Target pid missing in the config file."
	UNMARSHAL_ERROR = "Could not unmarshal incoming envelope."
)

const (
	ERROR = iota
	RUNNING
	STOPPED
)

const (
	KILL_TIMEOUT = 200
)

// Peer holds an id and an associated address for another server in the cluster.
type Peer struct {
	Pid  int
	Addr string
	sock *zmq.Socket
}

// Server is an object that represents a single server in the cluster.
type Server struct {
	pid       int
	state     int
	blacklist set
	addr      string
	sock      *zmq.Socket
	peers     map[int]Peer
	inbox     chan *Envelope
	outbox    chan *Envelope
	stop      chan bool
	stopped   chan bool
}

func init() {
	zmq.SetMaxSockets(20480)
}

// NewServer creates and returns a new Server object with the provided id and
// the peers provided in the config file.
func NewServer(id int, config string) (s *Server, err error) {
	INFO.Println(fmt.Sprintf("Creating server [id: %d, config: %s]", id, config))
	defer INFO.Println(fmt.Sprintf("Server creation returns [err: %s]", err))

	s = &Server{
		pid:       id,
		state:     ERROR,
		addr:      "",
		sock:      nil,
		blacklist: set{},
		peers:     make(map[int]Peer),
		inbox:     make(chan *Envelope, 16),
		outbox:    make(chan *Envelope, 16),
		stop:      make(chan bool, 2),
		stopped:   make(chan bool),
	}

	data, err := ioutil.ReadFile(config)
	if err != nil {
		EROR.Println(fmt.Sprintf("Error reading %s [err: %s]", config, err))
		return
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var p Peer
		if err = decoder.Decode(&p); err == io.EOF {
			err = nil
			break
		} else if err != nil {
			EROR.Println(fmt.Sprintf("Error parsing json [err: %s]", err))
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
		EROR.Println(fmt.Sprintf("Error creating server [err: %s]", err))
		return
	}

	s.state = STOPPED
	return
}

func (s *Server) Blacklist(bad_peers []int) {
	s.blacklist.clear()
	for peer := range bad_peers {
		if peer < len(s.peers) {
			s.blacklist.insert(peer)
		}
	}
}

// Returns the Pid's of all other servers in the same cluster as this server.
func (s *Server) Peers() []int {
	peers := make([]int, 0, len(s.peers))
	for k := range s.peers {
		peers = append(peers, k)
	}
	return peers
}

// Returns the Pid of the server in the cluster.
func (s *Server) Pid() int {
	return s.pid
}

// Returns the inbox channel of the server.
func (s *Server) Inbox() <-chan *Envelope {
	return s.inbox
}

// Returns the outbox channel of the server.
func (s *Server) Outbox() chan<- *Envelope {
	return s.outbox
}

// Start function attempts to bring up a stopped server.
func (s *Server) Start() (err error) {
	if s.state == STOPPED {
		s.state = RUNNING

		if s.sock, err = zmq.NewSocket(zmq.PULL); err != nil {
			EROR.Println(fmt.Sprintf("Error creation ZMQ socket [err: %s]", err))
			s.state = ERROR
			s.sock.Close()
			s.sock = nil
			return
		}

		if err = s.sock.Bind("tcp://" + s.addr); err != nil {
			EROR.Println(fmt.Sprintf("Error binding socket of %d [err: %s]", s.pid, err))
			s.state = ERROR
			s.sock.Close()
			s.sock = nil
			return
		}

		go s.monitorInbox()
		go s.monitorOutbox()
	}
	return
}

// Stop function attempts to bring down an active server gracefully.
func (s *Server) Stop() error {
	INFO.Println("Stopping server", s.pid)

	if s.state == RUNNING {
		s.state = STOPPED

		s.stop <- true // for monitorInbox
		s.stop <- true // for monitorOutbox

		sock, err := zmq.NewSocket(zmq.PUSH)
		if err != nil {
			return err
		}
		if err = sock.Connect("tcp://" + s.addr); err != nil {
			return err
		}
		sock.SendBytes([]byte{1}, 0)

		<-s.stopped
		sock.Close()
		sock = nil
	}

	for _, p := range s.peers {
		if p.sock != nil {
			p.sock.Close()
			p.sock = nil
		}
	}

	return nil
}

// Monitors the server's inbox channel for envelopes.
func (s *Server) monitorInbox() {
	INFO.Println(fmt.Sprintf("Monitoring inbox [id: %d]", s.pid))
	defer INFO.Println(fmt.Sprintf("Stopped inbox monitor [id: %d]", s.pid))

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
				EROR.Println("%s -- %s.", UNMARSHAL_ERROR, data)
			} else if !s.blacklist.has(envelope.Pid) {
				s.inbox <- &envelope
			}
		}
	}
}

// Monitors the server's outbox channel for envelopes.
func (s *Server) monitorOutbox() {
	INFO.Println(fmt.Sprintf("Monitoring outbox [id: %d]", s.pid))
	defer INFO.Println(fmt.Sprintf("Stopped outbox monitor [id: %d]", s.pid))

	for {
		select {
		case <-s.stop:
			return
		case envelope := <-s.outbox:
			if envelope.Pid != BROADCAST {
				go s.writeToServer(envelope.Pid, envelope)
			} else {
				for p := range s.peers {
					if !s.blacklist.has(p) {
						go s.writeToServer(p, envelope)
					}
				}
			}
		}
	}
}

// Reads data from the server's socket and sends them to the provided channel.
func (s *Server) readFromServer(dchan chan []byte) (err error) {
	INFO.Println(fmt.Sprintf("Reading server channel [id: %d]", s.pid))
	defer INFO.Println(fmt.Sprintf("Reading server channel returns [err: %s]", err))

	for {
		if reply, e := s.sock.RecvBytes(0); e != nil {
			// Ignore the error, we are in a go-routine
		} else if s.state == STOPPED {
			s.sock.Close()
			s.sock = nil

			s.stopped <- true
			return
		} else {
			dchan <- reply
		}
	}
}

// Sends an envelope to the peer with the provided peer id.
func (s *Server) writeToServer(pid int, env *Envelope) (err error) {
	INFO.Println(fmt.Sprintf("Sending envelope [from: %d, id: %d, env: %s]", s.pid, pid, env.toString()))
	defer INFO.Println(fmt.Sprintf("Sending envelope returns [err: %s]", err))

	p, ok := s.peers[pid]
	if !ok {
		WARN.Println(fmt.Sprintf("Error sending envelope %s [err: Invalid Peer ID]", env.toString()))
		err = fmt.Errorf("%s -- %s", INVALID_PEER_ID, pid)
	}

	env.Pid = s.pid
	msg := new(bytes.Buffer)
	err = gob.NewEncoder(msg).Encode(env)
	if err != nil {
		WARN.Println(fmt.Sprintf("Error encoding envelope %s [err: %s]", env, err))
		return
	}

	if p.sock == nil {
		if p.sock, err = zmq.NewSocket(zmq.PUSH); err != nil {
			EROR.Println(fmt.Sprintf("Error creating ZMQ socket to send %s [err: %s]", env, err))
			p.sock = nil
			return
		}
		p.sock.Connect("tcp://" + p.Addr)
	}
	p.sock.SendBytes(msg.Bytes(), 0)

	return
}
