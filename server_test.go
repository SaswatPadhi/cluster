package cluster

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"

	"bytes"
	"io"
	"testing"
	"time"
)

const (
	CONFIG_FILE = "cluster.json"
)

// Brings up a cluster with server descriptions as specified in CONFIG_FILE.
func ClusterSetup(t *testing.T) []*Server {
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

	return servers
}

// Brings down a cluster consisting of the slice of servers provided.
func ClusterTearDown(t *testing.T, servers []*Server) {
	for _, server := range servers {
		if err := server.Stop(); err != nil {
			t.Error(err)
		}
	}
}

// TEST: Checks if a cluster could be brought up and down successfully.
func Test_ClusterInitialize(t *testing.T) {
	ClusterTearDown(t, ClusterSetup(t))
}

// TEST: Checks if the cluster configuration could be brought up and down
//       multiple times. This might expose problems in cleanly tearing down
//       a running cluster.
func Test_ClusterMultiSetupDestroy(t *testing.T) {
	ClusterTearDown(t, ClusterSetup(t))
	ClusterTearDown(t, ClusterSetup(t))
	ClusterTearDown(t, ClusterSetup(t))
}

// TEST: Checks if a cluster with bad configuration can be initialized.
//       The only bad configuration we are checking now is if the server's id
//       is missing from the cluster configuration.
func Test_ClusterBadInitialize(t *testing.T) {
	servers := ClusterSetup(t)
	defer ClusterTearDown(t, servers)

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

// TEST: Checks if broadcast messages are correctly propagated.
func Test_ClusterBroadcast(t *testing.T) {
	servers := ClusterSetup(t)
	defer ClusterTearDown(t, servers)

	rand.Seed(101)
	env_broadcast_1_id := rand.Intn(len(servers))
	env_broadcast_1_msg, err := GenerateUUID()
	if err != nil {
		t.Fatalf(err.Error())
	}

	env_expected_1 := Envelope{servers[env_broadcast_1_id].pid, 0, string(env_broadcast_1_msg)}
	env_broadcast_1 := env_expected_1
	env_broadcast_1.Pid = BROADCAST

	env_broadcast_2_id := rand.Intn(len(servers))
	env_broadcast_2_msg, err := GenerateUUID()
	if err != nil {
		t.Fatalf(err.Error())
	}

	env_expected_2 := Envelope{servers[env_broadcast_2_id].pid, 0, string(env_broadcast_2_msg)}
	env_broadcast_2 := env_expected_2
	env_broadcast_2.Pid = BROADCAST

	servers[env_broadcast_1_id].Outbox() <- &env_broadcast_1
	servers[env_broadcast_2_id].Outbox() <- &env_broadcast_2

	for _, server := range servers {
		env_1_rvcd := server.pid == env_expected_1.Pid
		env_2_rcvd := server.pid == env_expected_2.Pid

		for !(env_1_rvcd && env_2_rcvd) {
			select {
			case envelope := <-server.Inbox():
				if *envelope == env_expected_1 {
					if !env_1_rvcd {
						env_1_rvcd = true
					} else {
						t.Errorf("[!] Broadcast envelope %s was received more than once at server [%d].\n", envelope, server.pid)
					}
				} else if *envelope == env_expected_2 {
					if !env_2_rcvd {
						env_2_rcvd = true
					} else {
						t.Errorf("[!] Broadcast envelope %s was received more than once at server [%d].\n", envelope, server.pid)
					}
				} else {
					t.Errorf("[!] Unexpected envelope %s was received at server [%d]!\n", envelope, server.pid)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("[!] Time out waiting for broadcast envelopes at server [%d]. Status = %t, %t.\n", server.pid, env_1_rvcd, env_2_rcvd)
			}
		}
	}

}