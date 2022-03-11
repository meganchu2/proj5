package surfstore

import (
	context "context"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// panic("todo")
	key := blockHash.Hash
	return bs.BlockMap[key], nil

	//sufstore grpc.go
	//create response object and put in content
	// need to change so not hardcoded
	// return &Block{
	// 	BlockData: []byte("Abcd"),
	// 	BlockSize: 4,
	// }, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// panic("todo")
	data := block.BlockData
	//size := block.BlockSize
	key := GetBlockHashString(data)
	bs.BlockMap[key] = block
	//set bool
	return &Success{}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// panic("todo")
	blockHashesOut := make([]string, 0)
	for _, key := range blockHashesIn.Hashes { // for each hash string
		if _, ok := bs.BlockMap[key]; ok { // if hash string in map
			//do something here
			blockHashesOut = append(blockHashesOut, key) // add to output
		}
	}
	temp := BlockHashes{}
	temp.Hashes = blockHashesOut
	return &temp, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
