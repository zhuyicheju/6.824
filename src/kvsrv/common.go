package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key        string
	Value      string
	Message_id int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key        string
	Term       uint32
	Message_id int64
}

type GetReply struct {
	Value string
}

type Report struct {
	Message_id int64
}
