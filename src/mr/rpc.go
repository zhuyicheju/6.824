package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	RPC_REPLY_WAIT = iota
	RPC_REPLY_MAP
	RPC_REPLY_REDUCE
	RPC_REPLY_DONE
)

const (
	RPC_SEND_REQUEST = iota
	RPC_SEND_DONE_MAP
	RPC_SEND_DONE_REDUCE
	RPC_SEND_ERROR
)

type request_t = int

type ArgsType struct {
	Send_type int
	ID        int
}

type ReplyType struct {
	Reply_type request_t
	ID         int
	File       string
	NReduce    int
	NMap       int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
