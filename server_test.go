package cluster

import (
	"encoding/json"
	"io/ioutil"

	"bytes"
	"io"
	"reflect"
	"testing"
	"time"
)

const (
	CONFIG_FILE = "cluster.json"

	MSG_EXPECT_TIMEOUT   = 250
	MSG_OVERFLOW_TIMEOUT = 50
)

/*=======================================< HELPER ROUTINES >=======================================*/

// Brings up a cluster with server descriptions as specified in CONFIG_FILE.
func ClusterSetup(t *testing.T, do_start bool) []*Server {
	data, err := ioutil.ReadFile(CONFIG_FILE)
	if err != nil {
		t.Fatal(err.Error())
	}

	servers := make([]*Server, 0, bytes.Count(data, []byte{10})+1)
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var p Peer
		if err := decoder.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
		}

		server, err := NewServer(p.Pid, CONFIG_FILE)
		if err != nil {
			t.Error(err)
		} else {
			servers = append(servers, server)
		}
	}

	if do_start {
		ClusterStart(t, servers)
	}
	return servers
}

// Starts a cluster consisting of the slice of servers provided.
func ClusterStart(t *testing.T, servers []*Server) {
	for _, server := range servers {
		if err := server.Start(); err != nil {
			t.Error(err)
		}
	}
}

// Stops a cluster consisting of the slice of servers provided.
func ClusterStop(t *testing.T, servers []*Server) {
	for _, server := range servers {
		if err := server.Stop(); err != nil {
			t.Error(err)
		}
	}
}

func CheckMessageFrom(msg interface{}, s_id int, r_id int, servers []*Server, res chan<- bool) {
	select {
	case env := <-servers[r_id].Inbox():
		res <- (servers[s_id].pid == env.Pid && msg == env.Msg)
	case <-time.After(MSG_EXPECT_TIMEOUT * time.Millisecond):
		res <- false
	}
	close(res)
}

// Checks for broadcast from a particular server.
func CheckBroadcastFrom(msg interface{}, s_id int, servers []*Server, res chan<- bool) {
	all := true
	left := -1
	cases := make([]reflect.SelectCase, len(servers)-1)

	servers[s_id].Outbox() <- &Envelope{
		Pid:   -1,
		MsgId: 0,
		Msg:   msg,
	}

	for i, _ := range servers {
		if i != s_id {
			left++
			ch := make(chan bool, 1)
			go CheckMessageFrom(msg, s_id, i, servers, ch)
			cases[left] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
		}
	}
	left++
	for left > 0 {
		_, val, ok := reflect.Select(cases)
		if ok {
			res <- val.Bool()
			all = all && val.Bool()
		} else {
			left--
		}
	}

	res <- all
	close(res)
}

/*========================================< TEST ROUTINES >========================================*/

// TEST: Checks if a cluster could be brought up and down successfully.
func Test_ClusterInitialize(t *testing.T) {
	ClusterStop(t, ClusterSetup(t, true))
}

// TEST: Checks if the cluster configuration could be brought up and down
//       multiple times. This might expose problems in cleanly tearing down
//       a running cluster.
func Test_ClusterMultiSetupDestroy(t *testing.T) {
	ClusterStop(t, ClusterSetup(t, true))
	ClusterStop(t, ClusterSetup(t, true))
	ClusterStop(t, ClusterSetup(t, true))
}

// TEST: Checks if the same cluster could be reused.
func Test_ClusterReuse(t *testing.T) {
	servers := ClusterSetup(t, false)

	ClusterStart(t, servers)
	ClusterStop(t, servers)

	ClusterStart(t, servers)
	ClusterStop(t, servers)

	ClusterStart(t, servers)
	ClusterStop(t, servers)
}

// TEST: Checks if a cluster with bad configuration can be initialized.
//       The only bad configuration we are checking now is if the server's id
//       is missing from the cluster configuration.
func Test_ClusterBadInitialize(t *testing.T) {
	servers := ClusterSetup(t, true)
	defer ClusterStop(t, servers)

	bad_pid := servers[0].Pid()
	for pid := range servers[0].Peers() {
		if pid > bad_pid {
			bad_pid = pid
		}
	}
	bad_pid++

	server, err := NewServer(bad_pid, CONFIG_FILE)
	if err == nil {
		t.Errorf("[!] Server creation with pid %d (missing in %s) was successful!\n", bad_pid, CONFIG_FILE)
	}

	server.Stop()
}

// TEST: A heavy broadcast test, broadcast from every server simultaneously.
func Test_ClusterBroadcast(t *testing.T) {
	servers := ClusterSetup(t, true)
	defer ClusterStop(t, servers)

	left := len(servers)
	chans := []chan bool{}
	cases := make([]reflect.SelectCase, len(servers))
	broadcast_from_results := make([]bool, len(servers))

	// Setup the channels and cases
	for id, _ := range servers {
		ch := make(chan bool, len(servers))
		chans = append(chans, ch)
		cases[id] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}

	// Asynchronously start broadcasts
	go func() {
		for id, _ := range servers {
			if msg, err := GenerateUUID(); err != nil {
				t.Error("[!] Failed to GenerateUUID().")
			} else {
				go CheckBroadcastFrom(string(msg), id, servers, chans[id])
				<-time.After(MSG_OVERFLOW_TIMEOUT * time.Millisecond)
			}
		}
	}()

	// Check the results
	for left > 0 {
		ch_id, val, ok := reflect.Select(cases)
		if ok {
			broadcast_from_results[ch_id] = val.Bool()
			if !val.Bool() {
				t.Errorf("[!] Failed to verify broadcast message from %d at one of the servers.", ch_id)
			}
		} else if !ok {
			//cases[ch_id].Chan = reflect.ValueOf(nil)
			left--
		}
	}
}
