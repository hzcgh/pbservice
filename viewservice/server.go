package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	view         View
	idle         string
	ack          bool
	c_primary    int
	c_backup     int
	dead_primary bool
	dead_backup  bool
	backupReady  bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Your code here.
	switch {

	case vs.view.Viewnum == 0:
		vs.view.Viewnum = 1
		vs.view.Primary = args.Me

		vs.ack = false

		reply.View.Viewnum = 0
		reply.View.Primary = args.Me

	case vs.view.Primary == args.Me:
		vs.c_primary = 0

		// Primary is down and the data is lost even though now it's pinging again, backup should be promoted to primary
		if args.Viewnum == 0 && vs.view.Viewnum!=1 { 	// special case here: server may be still pinging 0 at the beginning even though the view number has changed to 1
			vs.dead_primary = true
		}

		if vs.view.Viewnum == args.Viewnum && !vs.ack {
			vs.ack = true
		}

		reply.View = vs.view

	case vs.view.Backup == args.Me:
		vs.c_backup = 0
		
		// The backup server is uninitialized, thus it cannot be promoted to primary
		if args.Viewnum == 0 {
			vs.backupReady = false
		} else if args.Viewnum == vs.view.Viewnum {
			vs.backupReady = true
		}

		reply.View = vs.view

	case args.Me != vs.view.Primary && args.Me != vs.view.Backup:
		vs.idle = args.Me

		reply.View = vs.view

	}

	return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = vs.view
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()


	if vs.view.Primary != "" {
		if vs.c_primary == DeadPings {
			vs.c_primary = 0
			vs.dead_primary = true
		} else {
			vs.c_primary += 1
		}
	}
	if vs.view.Backup != "" {
		if vs.c_backup == DeadPings {
			vs.c_backup = 0
			vs.dead_backup = true
		} else {
			vs.c_backup += 1
		}
	}

	// Aggregate all the view change scenarios
	if vs.ack {

		isViewChanged := false

		if vs.dead_primary {
			vs.view.Primary = ""
			if vs.view.Backup != "" && vs.backupReady {
				vs.view.Primary = vs.view.Backup
				vs.view.Backup = ""
				vs.dead_primary = false
				isViewChanged = true
			}
		}

		if vs.dead_backup {
			vs.view.Backup = ""
			if vs.idle != "" {
				vs.view.Backup = vs.idle
				vs.idle = ""
				vs.backupReady  = false
				vs.dead_backup = false
				isViewChanged = true
			}
		}

		if vs.view.Backup == "" && vs.idle != "" {
			vs.view.Backup = vs.idle
			vs.idle = ""
			isViewChanged = true
		}

		if isViewChanged {
			vs.view.Viewnum += 1
			vs.ack = false
		}

	}

}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.

	vs.view = View{}
	vs.dead_backup = false
	vs.dead_primary = false

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
