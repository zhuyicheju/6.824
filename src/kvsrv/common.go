package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Term      uint32
	Client_id int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key       string
	Term      uint32
	Client_id int64
}

type GetReply struct {
	Value string
}

type Report struct {
	Client_id int64
}
