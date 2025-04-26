package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	kv map[string]string

	// client map[int64]uint32

	oldvalue map[int64]string
}

//	发送 结束
//
// client 1   1
// server 0   1
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.oldvalue[args.Message_id]
	if ok {
		reply.Value = val
		return
	}
	key := args.Key
	kv.oldvalue[args.Message_id] = kv.kv[key]
	reply.Value = kv.kv[key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.oldvalue[args.Message_id]
	if ok {
		reply.Value = ""
		return
	}
	key := args.Key
	value := args.Value
	reply.Value = ""
	kv.kv[key] = value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.oldvalue[args.Message_id]
	if ok {
		reply.Value = val
		return
	}
	key := args.Key
	value := args.Value
	oldvalue := kv.kv[key]
	kv.oldvalue[args.Message_id] = oldvalue
	reply.Value = oldvalue
	kv.kv[key] = oldvalue + value
}

func (kv *KVServer) Report(args *Report, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.oldvalue, args.Message_id)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.kv = make(map[string]string)

	kv.oldvalue = make(map[int64]string)

	return kv
}
