package cluster

import (
    zmq "github.com/pebbe/zmq4"

    "encoding/json"
    "io/ioutil"

    "errors"
    "fmt"
    "io"
    "log"
    "os"
    "strings"
)

const (
    BROADCAST = -1

    BAD_JSON_CONFIG = "JSON Decode failed on the config file."
    INVALID_SELF_ID = "The server's pid was not found in the config file."
    IO_ERROR_CONFIG = "Unable to locate config file."
    IP_BIND_FAILURE = "Could not bind to the specified address."
)

type Envelope struct {
    Pid   int
    MsgId int64 //unused currently
    Msg   interface{}
}

type Peer struct {
    Pid  int
    Addr string
}

type ServerI interface {
    Pid() int
    Peers() []int
    Inbox() chan *Envelope
    Outbox() chan *Envelope
}

type Server struct {
    pid            int
    addr           string
    sock           *zmq.Socket
    peers          map[int]Peer
    inbox          chan *Envelope
    outbox         chan *Envelope
    stop           chan bool
    stopped        chan bool
    release_inbox  chan bool
    terminate_code string
}

func GenerateUUID() string {
    dev_rand, _ := os.Open("/dev/urandom")
    rand_bytes := make([]byte, 16)
    dev_rand.Read(rand_bytes)
    dev_rand.Close()
    return fmt.Sprintf("%x-%x-%x-%x-%x", rand_bytes[0:4], rand_bytes[4:6], rand_bytes[6:8], rand_bytes[8:10], rand_bytes[10:])
}

func New(id int, config string) (*Server, error) {
    s := new(Server)
    s.pid = id
    s.stop = make(chan bool)
    s.peers = make(map[int]Peer)
    s.inbox = make(chan *Envelope)
    s.outbox = make(chan *Envelope)
    s.stopped = make(chan bool)
    s.release_inbox = make(chan bool)
    s.terminate_code = GenerateUUID()

    data, err := ioutil.ReadFile(config)
    if err != nil {
        return nil, errors.New(IO_ERROR_CONFIG + err.Error())
    }

    decoder := json.NewDecoder(strings.NewReader(string(data)))
    for {
        var p Peer
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

func (s *Server) writeToSocket(pid int, env *Envelope) {
    sock, _ := zmq.NewSocket(zmq.PUSH)
    defer sock.Close()

    env.Pid = s.pid
    msg, _ := json.Marshal(env)
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
    sock.Send(string(msg), 0)
}

func (s *Server) readFromSocket(dchan chan string) {
    for {
        if reply, err := s.sock.Recv(0); err != nil {
            log.Printf("[/] Error reading from socket of server [%d]. Stopping.\n", s.pid)
            s.sock.Close()
            s.stopped <- true
            return
        } else if reply == s.terminate_code {
            s.sock.Close()
            s.stopped <- true
            return
        } else {
            dchan <- reply
        }
    }
}

func (s *Server) monitorInbox() {
    data_chan := make(chan string)
    go s.readFromSocket(data_chan)

    for {
        select {
        case <-s.stop:
            return
        case data := <-data_chan:
            var envelope Envelope
            if err := json.Unmarshal([]byte(data), &envelope); err != nil {
                log.Printf("[!] Dropping incoming envelope %s -- Could not unmarshal.\n", data)
            } else {
                s.inbox <- &envelope
            }
        }
    }
}

func (s *Server) monitorOutbox() {
    for {
        select {
        case <-s.stop:
            return
        case envelope := <-s.outbox:
            if envelope.Pid != BROADCAST {
                go s.writeToSocket(envelope.Pid, envelope)
            } else {
                for p := range s.peers {
                    go s.writeToSocket(p, envelope)
                }
            }
        }
    }
}

func (s *Server) DumpInbox() {
    for {
        select {
        case <-s.release_inbox:
            return
        case envelope := <-s.inbox:
            msg, _ := json.Marshal(envelope)
            log.Printf("[>] New envelope in server [%d] inbox :: %s.\n", s.pid, msg)
        }
    }
}

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

func (s *Server) Pid() int {
    return s.pid
}

func (s *Server) Peers() []int {
    peers := make([]int, 0, len(s.peers))
    for k := range s.peers {
        peers = append(peers, k)
    }
    return peers
}

func (s *Server) Inbox() chan *Envelope {
    return s.inbox
}

func (s *Server) Outbox() chan *Envelope {
    return s.outbox
}
