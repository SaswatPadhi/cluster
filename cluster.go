package cluster

import (
	zmq "github.com/pebbe/zmq4"

	"encoding/gob"
	"encoding/json"
	"io/ioutil"

	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

const (
	// Envelope Pid for a broadcast message
	BROADCAST = -1

	// Error messages
	BAD_JSON_CONFIG = "JSON Decode failed on the config file."
	INVALID_SELF_ID = "The server's pid was not found in the config file."
	IO_ERROR_CONFIG = "Unable to locate config file."
	IP_BIND_FAILURE = "Could not bind to the specified address."
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
}

// ServerI is an interface that a Server object must provide.
type ServerI interface {
	Pid() int
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
// TODO: Error handling!
func GenerateUUID() string {
	dev_rand, _ := os.Open("/dev/urandom")
	rand_bytes := make([]byte, 16)
	dev_rand.Read(rand_bytes)
	dev_rand.Close()
	return fmt.Sprintf("%x-%x-4%x-b%x-%x", rand_bytes[0:4], rand_bytes[4:6],
		rand_bytes[6:8], rand_bytes[8:10], rand_bytes[10:])
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
	s.terminate_code = GenerateUUID()

	data, err := ioutil.ReadFile(config)
	if err != nil {
		return nil, errors.New(IO_ERROR_CONFIG + err.Error())
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var p peer
		if err := decoder.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.New(BAD_JSON_CONFIG + err.Error())
		} else if p.Pid == s.pid {
			s.addr = p.Addr
			continue
		}
		s.peers[p.Pid] = p
	}

	if len(s.addr) == 0 {
		return nil, errors.New(INVALID_SELF_ID)
	}

	sock, _ := zmq.NewSocket(zmq.PULL)
	if err := sock.Bind("tcp://*" + s.addr); err != nil {
		return nil, errors.New(IP_BIND_FAILURE)
	}
	s.sock = sock

	go s.monitorInbox()
	go s.monitorOutbox()

	return s, nil
}

// Sends an envelope to the peer with the provided peer id.
func (s *Server) writeToServer(pid int, env *Envelope) {
	sock, _ := zmq.NewSocket(zmq.PUSH)
	defer sock.Close()

	env.Pid = s.pid

	msg := new(bytes.Buffer)
	enc := gob.NewEncoder(msg)
	enc.Encode(env)
	if p, ok := s.peers[pid]; !ok {
		log.Printf("[?] Dropping outgoing envelope %s -- Pid not found.\n", msg)
		return
	} else {
		if err := sock.Connect("tcp://localhost" + p.Addr); err != nil {
			log.Printf("[!] Dropping outgoing envelope %s -- Could not connect to server [%d] @ %s.\n", msg, p.Pid, p.Addr)
			log.Printf("[!]   Error = %s.\n", err.Error())
			return
		}
	}
	sock.SendBytes(msg.Bytes(), 0)
}

// Reads data from the server's socket and sends them to the provided channel.
func (s *Server) readFromServer(dchan chan []byte) {
	for {
		if reply, err := s.sock.RecvBytes(0); err != nil {
			log.Printf("[/] Error reading from socket of server [%d]. Stopping.\n", s.pid)
			s.sock.Close()
			s.stopped <- true
			return
		} else if string(reply) == s.terminate_code {
			s.sock.Close()
			s.stopped <- true
			return
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
				log.Printf("[!] Dropping incoming envelope %s -- Could not unmarshal.\n", data)
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
func (s *Server) Stop() {
	s.stop <- true
	s.stop <- true

	sock, _ := zmq.NewSocket(zmq.PUSH)
	defer sock.Close()
	if err := sock.Connect("tcp://localhost" + s.addr); err != nil {
		log.Printf("[!] Looks like server [%d] has already closed it's socket.\n", s.pid)
		return
	}

	sock.Send(s.terminate_code, 0)
	<-s.stopped
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
