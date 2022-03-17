package surfstore

import (
	context "context"
	"time"
	"errors"
	"strings"
	grpc "google.golang.org/grpc"
	//"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// type RPCClient struct {
// 	MetaStoreAddr string
// 	BaseDir       string
// 	BlockSize     int
// }
type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server //surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials())?
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)// NewMetaStoreClient(conn)?

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// getblockstoreaddr(atc, &emptypb.Empty())
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = true

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	temp := BlockHashes{}
	temp.Hashes = blockHashesIn
	out, err := c.HasBlocks(ctx, &temp)
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = out.Hashes

	// close the connection
	return conn.Close()
}






func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// panic("todo")
	// connect to the server //surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials())?
	println("\nGETFILEINFO")
	leaderFound := false
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		mp, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				println("deadline exceeded")	
			} else if strings.Contains(err.Error(), "not the leader") {
				println("not leader")
				continue	
			} else if strings.Contains(err.Error(), "is crashed.") { // leader but crashed
				conn.Close()
				return err
			} else {
				conn.Close()
				return err
			}
		} else {
			leaderFound = true			
			println(len(mp.FileInfoMap))
			*serverFileInfoMap = mp.FileInfoMap
			break
		}
		
		// close the connection
		conn.Close()
	}
	if leaderFound {
		println("leader found")
	}
	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// panic("todo")
	println("\nUPDATEFILE")

	// crashed server cannot update file	
	// server := strconv.Atoi(surfClient.BaseDir[4:])
	// conn1, err1 := grpc.Dial(surfClient.MetaStoreAddrs[server], grpc.WithInsecure())
	// if err1 != nil {
	// 	return err1
	// }
	// c1 := NewRaftSurfstoreClient(conn)
	// ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
	// defer cancel1()
	// if state, _ := c1.IsCrashed(ctx1, &emptypb.Empty{}); state.IsCrashed {
	// 	return nil // don't sync if client is crashed
	// }

	leaderFound := false
	crashCount := 0
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		// c := NewMetaStoreClient(conn)// NewMetaStoreClient(conn)?

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		v, err := c.UpdateFile(ctx, fileMetaData)	
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				println("deadline exceeded")	
			} else if strings.Contains(err.Error(), "not the leader") {
				println("not leader")
				if state, _:=c.IsCrashed(ctx, &emptypb.Empty{}); state.IsCrashed {
					crashCount++
				}
				continue	
			} else if strings.Contains(err.Error(), "is crashed.") { // leader but crashed
				conn.Close()
				return err
			} else {
				conn.Close()
				return err
			}
		} else {
			leaderFound = true	
			*latestVersion = v.Version			
		}

		// close the connection
		conn.Close()
	}
	print("crashCount")
	println(crashCount)
	if crashCount > len(surfClient.MetaStoreAddrs) / 2 {
		return ERR_SERVER_CRASHED // majority of servers crashed
	}
	if leaderFound {
		println("leader found")	
	}
	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// panic("todo")
	// connect to the server //surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials())?
	println("\nGETBLOCKSTORE")
	
	leaderFound := false
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		// c := NewMetaStoreClient(conn)// NewMetaStoreClient(conn)?

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		temp := emptypb.Empty{}
		addr, err := c.GetBlockStoreAddr(ctx, &temp)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				println("deadline exceeded")	
			} else if strings.Contains(err.Error(), "not the leader") {
				println("not leader")
				continue	
			} else if strings.Contains(err.Error(), "is crashed.") { // leader but crashed
				conn.Close()
				return err
			} else {
				conn.Close()
				return err
			}
		} else {
			leaderFound = true			
			*blockStoreAddr = addr.Addr
			break
		}

		
		// close the connection
		conn.Close()
	}
	if leaderFound {
		println("leader found")
	}
	return nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
// func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) RPCClient {

// 	return RPCClient{
// 		MetaStoreAddr: hostPort,
// 		BaseDir:       baseDir,
// 		BlockSize:     blockSize,
// 	}
// }
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
			MetaStoreAddrs: addrs,
			BaseDir:       baseDir,
			BlockSize:     blockSize,
	}
}
