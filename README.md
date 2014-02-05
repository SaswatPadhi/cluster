cluster
=======

A simple cluster library implemented in Go.

As it's name says, `cluster` allows you to set up a basic cluster of machines.
`cluster` also allows you to send a job (wrapped in an `Envelope`), from any
node in the cluster to the other. It exposes a convenient set of methods that
allow clients to harness the cluster efficiently, without being bothered with
the underlying mechanisms.

## Install

```go
  go get github.com/SaswatPadhi/cluster
  go test github.com/SaswatPadhi/cluster
```

## Usage

`cluster` accepts cluster configurations as JSON objects which can be passed
while setting up servers. A cluster configuration file would look something
like:

```json
  {"Pid": 2345432, "Addr": "127.0.0.1:23454"}
  {"Pid": 3456543, "Addr": "127.0.0.1:34565"}
  {"Pid": 4567654, "Addr": "127.0.0.1:45676"}
```
The configuration file essentially maps server Pid's to the address they are
bound to, on the network.


A client would have to setup a local `Server` as below:

```go
server := cluster.NewServer(3456543,     // Unique Pid for this server
                            "conf.json") // cluster configuration in JSON
```
It is essential that this server should have an entry in the cluster
configuration file.

The `Server` object exposes the following set of methods:

  + `Pid()` -- Gives the Pid of the Server.
  + `Stop()` -- Gracefully terminates the Server.
  + `Peers()` -- Exposes the Pid's of all other servers.
  + `Inbox()` -- Returns a channel for reading incoming `Envelope`s.
  + `Outbox()` -- Returns a channel for writing outgoing `Envelope`s.

An `Envelope` is the basic unit of communication in `cluster`:

```go
env := cluster.Envelope(4567654,     // Receiver's Pid
                        1729,        // Global Message Id
                        "Hi There!") // The message to be sent.
```
If the receiver's Pid is set to `cluster.BROADCAST`, the envelope would be
broadcast to all servers in the cluster.

`cluster` uses [`gob`](http://golang.org/pkg/encoding/gob/) serialization on
[ZeroMQ](http://zeromq.org/) sockets for all communications. Big thanks to
[`pebbe/zmq4`](https://github.com/pebbe/zmq4) for Go bindings.


<br> <br>
- - - - -


### Disclaimer ::

This project is **WIP** (work-in-progress), use at your own risk!

Many of the features might be partially implemented and would *not* been
thoroughly tested. The author shall not be held responsible for alien
invasions, nuclear catastrophe or extinction of chipmunks; arising due to
the use of this software.
