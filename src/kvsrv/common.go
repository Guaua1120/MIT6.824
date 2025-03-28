package kvsrv

type MessageType int
const(
	Modify MessageType =0
	ACK MessageType =1
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Version int64
	Message MessageType

	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	Version int64
	Message MessageType
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
