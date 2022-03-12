package surfstore

import (
	context "context"
	"time"
	
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
	println("getfileinfo")
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
		mp, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			return err
		}
		*serverFileInfoMap = mp.FileInfoMap
		
		// close the connection
		conn.Close()
	}
	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// panic("todo")
	println("updatefile")
	// connect to the server //surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials())
	
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
			conn.Close()
			return err
		}
		*latestVersion = v.Version

		// close the connection
		conn.Close()
	}
	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// panic("todo")
	// connect to the server //surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials())?
	println("getblockstore")
	
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
			conn.Close()
			return err
		}
		*blockStoreAddr = addr.Addr

		
		// close the connection
		conn.Close()
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
