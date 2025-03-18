package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
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
	OperationId := nrand()
	args := GetArgs{
		Key: key,
		Version: OperationId,
		Message: Modify, 
	}
	reply :=GetReply{}
	for{
		ok :=ck.server.Call("KVServer.Get",&args,&reply)
		if ok {
			break;
		}
	}
	//call success之后告诉服务器清除map中记录的operation
	args = GetArgs{
		Version: OperationId,
		Message: ACK, 
	}
	for{
		ok :=ck.server.Call("KVServer.Get",&args,&GetReply{})
		if ok {
			break;
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
	// You will have to modify this function.
	OperationId := nrand()
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Version: OperationId,
		Message: Modify,
	}
	reply := PutAppendReply{}
	for{
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			break;
		}
	}
	args = PutAppendArgs{
		Version: OperationId,
		Message: ACK,
	}
	for{
		ok := ck.server.Call("KVServer."+op, &args, &PutAppendReply{})
		if ok {
			break;
		}
	}
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
// Append 方法在旧值后面追加
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
