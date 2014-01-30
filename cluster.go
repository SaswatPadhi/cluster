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
	"os"
)

const (
	// Envelope Pid for a broadcast message
	BROADCAST = -1

	// Error messages
	INVALID_SELF_ID = "Server's pid missing in the config file."
	INVALID_PEER_ID = "Target pid missing in the config file."
	UNMARSHAL_ERROR = "Could not unmarshal incoming envelope."
)

// Envelope represents a unit message that can be exchanged between servers.
// The Pid field represents the id of sender server for incoming messages; or
// the id of the target server for outgoing messages.
// The Pid can also be set to BROADCAST for an outgoing message, which would
// indicate that the envelope should be broadcast to all servers in the cluster.
type Envelope struct {
	Pid   int
	MsgId int64 //unused currently
	Msg   interface{}
}

// Peer holds an id and an associated address for another server in the cluster.
type peer struct {
	Pid  int
	Addr string
	sock *zmq.Socket
}

// ServerI is an interface that a Server object must provide.
type ServerI interface {
	Pid() int
	Stop()
	Peers() []int
	Inbox() chan *Envelope
	Outbox() chan *Envelope
}

// Server is an object that represents a single server in the cluster. This
// object implements the ServerI interface.
type Server struct {
	pid            int
	addr           string
	sock           *zmq.Socket
	peers          map[int]peer
	inbox          chan *Envelope
	outbox         chan *Envelope
	stop           chan bool
	stopped        chan bool
	terminate_code string
}

// Generates a Version 4 (pseudo-random) UUID (Universally Unique Identifier).
// This function is used to generate unique an terminate_code for a server and
// may be used for tests.
func GenerateUUID() (string, error) {
	dev_rand, err := os.Open("/dev/urandom")
	if err != nil {
		return "", err
	}

	rand_bytes := make([]byte, 16)
	if _, err = dev_rand.Read(rand_bytes); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x-%x-4%x-b%x-%x", rand_bytes[0:4], rand_bytes[4:6],
			rand_bytes[6:8], rand_bytes[8:10], rand_bytes[10:]),
		dev_rand.Close()
}

// NewServer creates and returns a new Server object with the provided id and
// the peers provided in the config file.
// In case of an error, this function should return a nil server and a non-nil
// error.
func NewServer(id int, config string) (*Server, error) {
	s := new(Server)
	s.pid = id
	s.stop = make(chan bool)
	s.peers = make(map[int]peer)
	s.inbox = make(chan *Envelope)
	s.outbox = make(chan *Envelope)
	s.stopped = make(chan bool)

	data, err := ioutil.ReadFile(config)
	if err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var p peer
		if err := decoder.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		} else if p.Pid == s.pid {
			s.addr = p.Addr
			continue
		}
		if p.sock, err = zmq.NewSocket(zmq.PUSH); err != nil {
			return nil, err
		}
		p.sock.Connect("tcp://" + p.Addr)
		s.peers[p.Pid] = p
	}

	if len(s.addr) == 0 {
		return nil, fmt.Errorf(INVALID_SELF_ID)
	}

	if s.terminate_code, err = GenerateUUID(); err != nil {
		return nil, err
	}

	if s.sock, err = zmq.NewSocket(zmq.PULL); err != nil {
		return nil, err
	}

	go s.monitorInbox()
	go s.monitorOutbox()

	return s, s.sock.Bind("tcp://" + s.addr)
}

// Sends an envelope to the peer with the provided peer id.
func (s *Server) writeToServer(pid int, env *Envelope) error {
	p, ok := s.peers[pid]
	if !ok {
		return fmt.Errorf("%s -- %s", INVALID_PEER_ID, pid)
	}

	env.Pid = s.pid
	msg := new(bytes.Buffer)
	gob.NewEncoder(msg).Encode(env)
	p.sock.SendBytes(msg.Bytes(), 0)

	return nil
}

// Reads data from the server's socket and sends them to the provided channel.
func (s *Server) readFromServer(dchan chan []byte) error {
	for {
		if reply, err := s.sock.RecvBytes(0); err != nil {
			return err
		} else if string(reply) == s.terminate_code {
			s.sock.Close()
			s.stopped <- true // for s.Stop()
			return nil
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

	sock.Send(s.terminate_code, 0)
	<-s.stopped

	return sock.Close()
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
func (s *Server) Inbox() chan *Envelope {
	return s.inbox
}

// Returns the outbox channel of the server.
func (s *Server) Outbox() chan *Envelope {
	return s.outbox
}
