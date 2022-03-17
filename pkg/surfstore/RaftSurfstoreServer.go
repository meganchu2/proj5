package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"sync"
    "math"
    "time"
    "google.golang.org/grpc"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	/////////////////////////////
	// added from discussion
	commitIndex int64
    pendingCommits []chan bool

    lastApplied int64

    // Server Info
    ip string
    ipList []string
    serverId int64

    // Leader protection
    isLeaderMutex sync.RWMutex
    isLeaderCond *sync.Cond

    rpcClients []RaftSurfstoreClient
	//////////////////////////
	//////////////////////////

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")
    if !s.isLeader {
        return nil, ERR_NOT_LEADER
    } else if s.isCrashed { // leader but crashed
        return nil, ERR_SERVER_CRASHED
    }
    // print("leader is ")
    // println(s.serverId)
    // consult majority of servers
    notCrashed := 1
    for notCrashed <= len(s.ipList) / 2 {
        for idx, addr := range s.ipList {
            if int64(idx) == s.serverId {
                continue
            }
            conn, err := grpc.Dial(addr, grpc.WithInsecure())
            if err != nil {
                //crashed
                continue
            }
            client := NewRaftSurfstoreClient(conn)
            if state, _ := client.IsCrashed(ctx, empty); !state.IsCrashed {
                // print(idx)
                // println("not crashed")
                notCrashed++
            }
        }
    }
    return s.metaStore.GetFileInfoMap(ctx, empty)
	//return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	//panic("todo")
    if !s.isLeader {
        return nil, ERR_NOT_LEADER
    } else if s.isCrashed { // leader but crashed
        return nil, ERR_SERVER_CRASHED
    }
    // print("leader is ")
    // println(s.serverId)
    // consult majority of servers
    notCrashed := 1
    for notCrashed <= len(s.ipList) / 2 {
        for idx, addr := range s.ipList {
            if int64(idx) == s.serverId {
                continue
            }
            conn, err := grpc.Dial(addr, grpc.WithInsecure())
            if err != nil {
                //crashed
                continue
            }
            client := NewRaftSurfstoreClient(conn)
            if state, _ := client.IsCrashed(ctx, empty); !state.IsCrashed {
                // print(idx)
                // println("not crashed")
                notCrashed++
            }
        }
    }
    return s.metaStore.GetBlockStoreAddr(ctx, empty)
	//return nil, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	//panic("todo")

	///////////////////////////
	// ///////////////////////////
    if !s.isLeader {
        return nil, ERR_NOT_LEADER
    } else if s.isCrashed { // leader but crashed
        return nil, ERR_SERVER_CRASHED
    }
	op := UpdateOperation{
        Term: s.term,
        FileMetaData: filemeta,
    }

    s.log = append(s.log, &op)
    committed := make(chan bool)
    s.pendingCommits = append(s.pendingCommits, committed)

    go s.attemptCommit()

    success := <-committed
    print("success")
    if success {
        println("Leader updated server", s.serverId)
        return s.metaStore.UpdateFile(ctx, filemeta)
    }
	///////////////////////////
	///////////////////////////

	return &Version{}, ERR_SERVER_CRASHED // not enough servers so fail
}

func (s *RaftSurfstore) attemptCommit() {
    targetIdx := s.commitIndex + 1
    commitChan := make(chan *AppendEntryOutput, len(s.ipList))
    for idx, _ := range s.ipList {
        if int64(idx) == s.serverId {
            continue
        }
        go s.commitEntry(int64(idx), targetIdx, commitChan)
    }
        
    commitCount := 1 // count self
    for commitCount < len(s.ipList) {
        // TODO handle crashed nodes
        commit := <-commitChan
        if commit != nil && commit.Success {
            //println("here1")
            commitCount++
        } 
        if commitCount > len(s.ipList) / 2 {
            s.pendingCommits[len(s.pendingCommits)-1] <- true
            //println("here2")
            s.commitIndex = targetIdx
            break
        }
    }
}


func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
    for {
        addr := s.ipList[serverIdx]
        conn, err := grpc.Dial(addr, grpc.WithInsecure())
        if err != nil {
            return
        }
        client := NewRaftSurfstoreClient(conn)
        
        // TODO create correct AppendEntryInput from s.nextIndex, etc
        input := &AppendEntryInput{
            Term: s.term,
            PrevLogTerm: -1,
            PrevLogIndex: -1,
            Entries: s.log[:entryIdx+1],
            LeaderCommit: entryIdx,//s.commitIndex,
        }   
        if entryIdx > 0 {
            input.PrevLogTerm = s.log[entryIdx - 1].Term
            input.PrevLogIndex = entryIdx - 1
        }
        
        ctx, cancel := context.WithTimeout(context.Background(), time.Second)
        defer cancel()
        output, _ := client.AppendEntries(ctx, input)
        println("appendentries complete for server")
        
        if output == nil || !output.Success {
            println("no success append entries")
            commitChan <- nil
            println("channel committed")
            return
        }
        if output.Success {
            commitChan <- output
            println("channel committed")
            return
        }
        // TODO update state. s.nextIndex, etc

        // TODO handle crashed/ non success cases
    }
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	//panic("todo")

    s.isLeaderMutex.Lock()
    s.isLeader = false
    s.isLeaderMutex.Unlock()
	
    output := &AppendEntryOutput{
        Success: false,
        MatchedIndex: -1,
    }

    for s.isCrashed{ // don't update until recovered
        //print("not updating")
        //println(s.serverId)
        //return output, nil
    }

    if input.LeaderCommit == -2 { // just wanted to update leader
        return output, nil
    }
    
    if input.Term >= s.term {
        s.term = input.Term
    }

    //1. Reply false if term < currentTerm (§5.1)
    if input.Term < s.term {
        return output, nil
    }
    //2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
    if input.PrevLogIndex >= 0 && (int64(len(s.log)) <= input.PrevLogIndex || s.log[input.PrevLogIndex].Term != input.PrevLogTerm) {
        return output, nil
    }
    //3. If an existing entry conflicts with a new one (same index but different
    //terms), delete the existing entry and all that follow it (§5.3)
    k := 0
    for i, entry := range input.Entries {
        if len(s.log) > i && s.log[i].Term != entry.Term {
            s.log = s.log[:i]
            break
        }
        if len(s.log) <= i{
            break
        }
        k++
    }
    //4. Append any new entries not already in the log
    s.log = append(s.log, input.Entries[k:]...)
    //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
    //of last new entry)
    // TODO only do this if leaderCommit > commitIndex
    if input.LeaderCommit > s.commitIndex {
        s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log) - 1)))
    }
    // println(s.serverId)
    // println(s.lastApplied)
    // print(s.commitIndex)
    // println(len(s.log))
    // println(len(input.Entries))
    for s.lastApplied < s.commitIndex {
        s.lastApplied++
        entry := s.log[s.lastApplied]
        s.metaStore.UpdateFile(ctx, entry.FileMetaData)
        println("updated server", s.serverId)
    }

    output.Success = true
    
    return output, nil
	//return nil, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//panic("todo")
	
    //////////////////////////
    ///////////////////////////
    if s.isCrashed {
        return &Success{Flag: false}, ERR_SERVER_CRASHED
    }

    s.isLeaderMutex.Lock()
    s.isLeader = true
	s.isLeaderMutex.Unlock()
    s.term++
    print("leader set to")
    println(s.serverId)

    // call appendentries to set isLeader to false
    for idx, addr := range s.ipList {
        if int64(idx) == s.serverId {
            continue
        }

        conn, err := grpc.Dial(addr, grpc.WithInsecure())
        if err != nil {
            //crashed
            continue
        }

        client := NewRaftSurfstoreClient(conn)

        input := &AppendEntryInput{
            Term: s.term,
            PrevLogTerm: -1,
            PrevLogIndex: -1,
            // TODO figure out which entries to send
            Entries: make([]*UpdateOperation, 0),
            LeaderCommit: -2,
        }
        ctx, cancel := context.WithTimeout(context.Background(), time.Second)
        defer cancel()
        _, _ = client.AppendEntries(ctx, input)
    }
	return &Success{Flag: true}, nil
    /////////////////////////////
    /////////////////////////////
    
    //return nil, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//panic("todo")

    ///////////////////////////
    ///////////////////////////
    if !s.isLeader {
        return &Success{Flag: false}, nil
    }
    for idx, addr := range s.ipList {
        if int64(idx) == s.serverId {
            continue
        }

        conn, err := grpc.Dial(addr, grpc.WithInsecure())
        if err != nil {
            // return nil, nil
            continue
        }

        client := NewRaftSurfstoreClient(conn)
	    
        // TODO create correct AppendEntryInput from s.nextIndex, etc
        input := &AppendEntryInput{
            Term: s.term,
            PrevLogTerm: -1,
            PrevLogIndex: -1,
            // TODO figure out which entries to send
            Entries: make([]*UpdateOperation, 0),
            LeaderCommit: s.commitIndex,
        }
        if len(s.log) > 0{
            input.PrevLogTerm = s.log[len(s.log) - 1].Term
            input.PrevLogIndex = int64(len(s.log) - 1)
        }

        ctx, cancel := context.WithTimeout(context.Background(), time.Second)
        defer cancel()
        output, err := client.AppendEntries(ctx, input)
        if output != nil {
            // server is alive
        }
    }

	return &Success{Flag: true}, nil
    ///////////////////////////
    ///////////////////////////

	//return nil, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()
    print(s.serverId)
    println("crashed")

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()
    print(s.serverId)
    println("restored")

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	println("getting internal state")
    fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
    println("obtained internal state")
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
