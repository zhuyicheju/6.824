package kvsrv

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	server     *labrpc.ClientEnd
	client_id  int64
	request_id uint32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.client_id = nrand()
	ck.request_id = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.request_id++
	args := GetArgs{Key: key, Term: ck.request_id, Client_id: ck.client_id}
	reply := GetReply{}
	ok := false
	for !ok {
		reply = GetReply{}
		ok = ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			report_ok := false
			report_args := Report{Client_id: ck.client_id}
			report_reply := GetReply{}
			for !report_ok {
				report_ok = ck.server.Call("KVServer.Report", &report_args, &report_reply)
			}
		}
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	ck.request_id++
	args := PutAppendArgs{Key: key, Value: value, Term: ck.request_id, Client_id: ck.client_id}
	reply := PutAppendReply{}
	ok := false
	for !ok {
		reply = PutAppendReply{}
		ok = ck.server.Call(fmt.Sprintf("KVServer.%v", op), &args, &reply)
		if ok {
			report_ok := false
			report_args := Report{Client_id: ck.client_id}
			report_reply := GetReply{}
			for !report_ok {
				report_ok = ck.server.Call("KVServer.Report", &report_args, &report_reply)
			}
		}
	}
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
