package kvpaxos

import (
	"cse-513/src/paxos"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key         string
	Val         string
	OrderNumber int64
	WhatOp      string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.

	DB          map[string]string
	CompletedOp map[int64]bool //completed ops , to avoid duplicates
	SEQ         int
}

func (kv *KVPaxos) StoreInDB(A Op) {
	kv.CompletedOp[A.OrderNumber] = true
	kv.px.Done(kv.SEQ)
	kv.SEQ++
	if A.WhatOp == "Append" {
		if CurrentVal, exist := kv.DB[A.Key]; exist {
			kv.DB[A.Key] = CurrentVal + A.Val
		} else {
			kv.DB[A.Key] = A.Val
		}
	} else if A.WhatOp == "Put" {
		kv.DB[A.Key] = A.Val
	}

}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	Operation := Op{Key: args.Key, OrderNumber: args.Order}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.CompletedOp[Operation.OrderNumber]; exists {
		fmt.Printf("Operation number %d is already completed.\n", Operation.OrderNumber)
		reply.Err = OK
		reply.Value = kv.DB[args.Key]
		return nil
	} else {
		for {
			kv.px.Start(kv.SEQ, Operation)
			state, val := kv.px.Status(kv.SEQ)
			if state == paxos.Decided {
				kv.StoreInDB(val.(Op))

				if Operation.OrderNumber == val.(Op).OrderNumber {
					if val, exist := kv.DB[args.Key]; exist {
						reply.Value = val
						reply.Err = OK
					} else {
						reply.Err = ErrNoKey
						reply.Value = ""
					}
					break
				} else {
					continue
				}

			} else if state == paxos.Pending {
				to := 10 * time.Millisecond

				for {
					time.Sleep(to)
					state, val := kv.px.Status(kv.SEQ)

					if state == paxos.Decided {
						kv.StoreInDB(val.(Op))

						if Operation.OrderNumber == val.(Op).OrderNumber {
							if val, exist := kv.DB[args.Key]; exist {
								reply.Value = val
								reply.Err = OK
							} else {
								reply.Err = ErrNoKey
								reply.Value = ""
							}
							return nil
						} else {
							break
						}
					}
					if to < 10*time.Second {
						to *= 2

					} else {
						reply.Err = ErrPending
						reply.Value = ""
						return nil
					}
				}
			} else if state == paxos.Forgotten {
				reply.Err = ErrForgotten
				reply.Value = ""
				return nil
			}
		}

	}

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	Operation := Op{Key: args.Key, Val: args.Value, OrderNumber: args.Order, WhatOp: args.Op}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.CompletedOp[Operation.OrderNumber]; exists {
		fmt.Printf("Operation number %d is already completed.\n", Operation.OrderNumber)
		reply.Err = OK
		return nil
	} else {
		for {

			kv.px.Start(kv.SEQ, Operation)
			state, val := kv.px.Status(kv.SEQ)

			if state == paxos.Decided {
				kv.StoreInDB(val.(Op))

				if Operation.OrderNumber == val.(Op).OrderNumber {
					reply.Err = OK
					break
				} else {
					continue
				}

			} else if state == paxos.Pending {
				to := 10 * time.Millisecond
				for {
					time.Sleep(to)

					state, val := kv.px.Status(kv.SEQ)

					if state == paxos.Decided {
						kv.StoreInDB(val.(Op))

						if Operation.OrderNumber == val.(Op).OrderNumber {
							reply.Err = OK
							return nil
						} else {
							break
						}
					}

					if to < 10*time.Second {
						to *= 2
					} else {
						reply.Err = ErrPending
						return nil
					}

				}
			} else if state == paxos.Forgotten {
				reply.Err = ErrForgotten
				return nil
			}

		}
	}

	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.DB = make(map[string]string)
	kv.CompletedOp = make(map[int64]bool)
	kv.SEQ = 0
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
