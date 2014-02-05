package cluster_test

import (
	"github.com/SaswatPadhi/cluster"

	"fmt"
)

// This example explains how to start and stop a Server instance.
func ExampleNewServer() {
	server, err := cluster.NewServer(
		1332,           // unique id of the server on the cluster
		"cluster.json") // cluster configuration file
	if err != nil {
		fmt.Printf("Server initialization failed: %s\n", err.Error())
	} else {
		fmt.Printf("Server initialization succeeded.\n")
		if err = server.Stop(); err != nil {
			fmt.Printf("Server shut down failed: %s\n", err.Error())
		} else {
			fmt.Printf("Server shut down succeeded.\n")
		}
	}
	// Output:
	// Server initialization succeeded.
	// Server shut down succeeded.
}
