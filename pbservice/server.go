package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.

	view       viewservice.View
	am_primary bool
	am_backup  bool
	kv         map[string]string

	oldBackup string
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	reply.Value = pb.kv[args.Key]

	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	reply.Err = OK

	fmt.Printf("Put - Me: %v, am_primary: %v, am_backup: %v, args: %v\n",pb.me,pb.am_primary, pb.am_backup, args)

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pb.kv[args.Key] = args.Value

	// Forward Put to the backup server
	if pb.am_primary {
		call(pb.view.Backup, "PBServer.Put", args, &reply)
	}

	return nil
}

func (pb *PBServer) Init(args *InitArgs, reply *InitReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	reply.Err = OK

	pb.kv = args.Kv

	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//fmt.Printf("%+v\n", pb)
	
	fmt.Printf("Me: %v, kv: %v\n",pb.me,pb.kv)

	v, _ := pb.vs.Ping(pb.view.Viewnum)

	//	fmt.Printf("%+v\n", v)
	pb.view = v

	// For primary server
	if pb.me == v.Primary {
		pb.am_primary = true

		if pb.oldBackup != v.Backup {
			if v.Backup != "" {
				// the backup server has changed, so the primary server should forward the whole database
				args := &InitArgs{Kv: pb.kv}
				call(pb.view.Backup, "PBServer.Init", args, &InitReply{})				
			}
			pb.oldBackup = v.Backup
		}

	} else {
		pb.am_primary = false
	}

	// For backup server
	if pb.me == v.Backup {
		pb.am_backup = true
	} else {
		pb.am_backup = false
	}

}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.view = viewservice.View{}
	pb.am_backup = false
	pb.am_primary = false
	pb.kv = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					fmt.Println("Unreliable - discard the request")
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					fmt.Println("Unreliable - process the request but force discard of reply")
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
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
