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
	// Your definitions here.
	kvMap map[string]string  //存储kv键值对的map
	versionMap map[int64]string   //存储操作id对应的返回结果
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//如果是收到ACK 直接返回
	if args.Message==ACK {
		delete(kv.versionMap, args.Version)
		return 
	}
	//如果是重复请求，直接返回versionMap中的值
	oldValue,exist := kv.versionMap[args.Version]
	if exist{
		reply.Value = oldValue
		return
	}
	reply.Value=kv.kvMap[args.Key]
	kv.versionMap[args.Version] = kv.kvMap[args.Key]
}

//install or replace
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Message==ACK{
		delete(kv.versionMap, args.Version)
		return 
	}
	oldValue,exist := kv.versionMap[args.Version]
	if exist{
		reply.Value = oldValue
		return
	}
	kv.kvMap[args.Key] = args.Value
	kv.versionMap[args.Version] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Message==ACK{
		delete(kv.versionMap, args.Version)
		return 
	}
	oldValue,exist := kv.versionMap[args.Version]
	if exist{
		reply.Value = oldValue
		return
	}
	reply.Value = kv.kvMap[args.Key]
	kv.versionMap[args.Version] = kv.kvMap[args.Key]
	kv.kvMap[args.Key] += args.Value
}

func StartKVServer() *KVServer {
	kv := KVServer{
		kvMap: make(map[string]string),
		versionMap: make(map[int64]string),
	}
	// You may need initialization code here.

	return &kv
}
