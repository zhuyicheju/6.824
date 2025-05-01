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
	kv    map[string]string
	mu_kv sync.Mutex

	// client map[int64]uint32

	oldvalue    map[int64]string
	mu_oldvalue sync.Mutex
}

//	发送 结束
//
// client 1   1
// server 0   1
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	id := args.Message_id
	kv.mu_oldvalue.Lock()
	val, ok := kv.oldvalue[id]
	kv.mu_oldvalue.Unlock()
	if ok {
		reply.Value = val
		return
	}
	key := args.Key

	kv.mu_kv.Lock()
	value := kv.kv[key]
	kv.mu_kv.Unlock()

	kv.mu_oldvalue.Lock()
	kv.oldvalue[id] = value
	kv.mu_oldvalue.Unlock()

	reply.Value = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	id := args.Message_id
	kv.mu_oldvalue.Lock()
	_, ok := kv.oldvalue[id]
	kv.mu_oldvalue.Unlock()
	if ok {
		reply.Value = ""
		return
	}
	key := args.Key
	value := args.Value
	reply.Value = ""

	kv.mu_kv.Lock()
	kv.kv[key] = value
	kv.mu_kv.Unlock()

	kv.mu_oldvalue.Lock()
	kv.oldvalue[id] = ""
	kv.mu_oldvalue.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	id := args.Message_id
	kv.mu_oldvalue.Lock()
	val, ok := kv.oldvalue[id]
	kv.mu_oldvalue.Unlock()
	if ok {
		reply.Value = val
		return
	}
	key := args.Key
	value := args.Value

	kv.mu_kv.Lock()
	oldvalue := kv.kv[key]
	kv.kv[key] = oldvalue + value
	kv.mu_kv.Unlock()

	reply.Value = oldvalue

	kv.mu_oldvalue.Lock()
	kv.oldvalue[id] = oldvalue
	kv.mu_oldvalue.Unlock()
}

func (kv *KVServer) Report(args *Report, reply *GetReply) {
	kv.mu_oldvalue.Lock()
	delete(kv.oldvalue, args.Message_id)
	kv.mu_oldvalue.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.kv = make(map[string]string)

	kv.oldvalue = make(map[int64]string)

	return kv
}
