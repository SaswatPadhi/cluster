package cluster

import ()

const (
	// Envelope Pid for a broadcast message
	BROADCAST = -1
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
