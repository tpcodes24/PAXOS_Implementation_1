package kvpaxos

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
)

type Clerk struct {
	servers []string
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{Key: key, Order: nrand()}

	ID := -1 //ID refers to ID of peer
	for {
		ID = (ID + 1) % len(ck.servers)
		var reply GetReply
		ok := call(ck.servers[ID], "KVPaxos.Get", &args, &reply)
		if ok {
			return reply.Value
		}

	}
}

// shared by Put and Append.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Order: nrand(), Value: value, Op: op}

	ID := -1
	for {
		ID = (ID + 1) % len(ck.servers)
		var reply PutAppendReply
		ok := call(ck.servers[ID], "KVPaxos.PutAppend", &args, &reply)
		if ok {
			break
		}

	}

}

func (ck *Clerk) Put(key string, value string) {
	// fmt.Printf("iamm\n")
	ck.PutAppend(key, value, "Put")
	// fmt.Printf("howwwww\n")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
