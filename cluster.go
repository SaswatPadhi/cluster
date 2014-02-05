/*
	cluster -- A simple cluster library in Go.
	(https://github.com/SaswatPadhi/cluster)

	As it's name says, `cluster` allows you to set up a basic cluster of machines.
	`cluster` also allows you to send a job (wrapped in an `Envelope`), from any
	node in the cluster to the other. It exposes a convenient set of methods that
	allow clients to harness the cluster efficiently, without being bothered with
	the underlying mechanisms.
*/
package cluster
