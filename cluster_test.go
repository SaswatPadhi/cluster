package cluster

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"

	"bytes"
	"io"
	"sort"
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
		t.Fatal("[X] " + IO_ERROR_CONFIG + "(" + CONFIG_FILE + ")")
	}

	servers := make([]*Server, 0, bytes.Count(data, []byte{10})+1)
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var p peer
		if err := decoder.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			t.Error("[!] " + err.Error())
		}

		t.Logf("[+] Setting up server instance [%d] @ %s.\n", p.Pid, p.Addr)
		server, err := NewServer(p.Pid, CONFIG_FILE)
		if err != nil {
			t.Error("[!] " + err.Error())
		} else {
			servers = append(servers, server)
		}
	}

	return servers
}

// Brings down a cluster consisting of the slice of servers provided.
func ClusterTearDown(t *testing.T, servers []*Server) {
	for _, server := range servers {
		t.Logf("[-] Destroying server instance [%d] @ %s.\n", server.pid, server.addr)
		server.Stop()
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
	pids := append(servers[0].Peers(), servers[0].Pid())
	sort.Ints(pids)
	bad_pid := pids[len(pids)-1] + 1
	server, err := NewServer(bad_pid, CONFIG_FILE)
	if (err == nil) != (server != nil) {
		t.Errorf("[!] Inconsistent return values from Server.New()!\n", bad_pid, CONFIG_FILE)
	} else if err == nil {
		t.Errorf("[!] Server creation with pid %d (missing in %s) was successful!\n", bad_pid, CONFIG_FILE)
	}
	ClusterTearDown(t, servers)
}

// TEST: Checks if broadcast messages are correctly propagated.
func Test_ClusterBroadcast(t *testing.T) {
	servers := ClusterSetup(t)

	rand.Seed(101)
	env_broadcast_1_id := rand.Intn(len(servers))
	env_expected_1 := Envelope{servers[env_broadcast_1_id].pid, 0, GenerateUUID()}
	env_broadcast_1 := env_expected_1
	env_broadcast_1.Pid = BROADCAST

	env_broadcast_2_id := rand.Intn(len(servers))
	env_expected_2 := Envelope{servers[env_broadcast_2_id].pid, 0, GenerateUUID()}
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
				t.Errorf("[!] Time out waiting for broadcast envelopes at server [%d]. Status = %s, %s.\n", server.pid, env_1_rvcd, env_2_rcvd)
			}
		}
	}

	ClusterTearDown(t, servers)
}
