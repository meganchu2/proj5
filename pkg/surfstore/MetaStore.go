package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"fmt"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	temp := FileInfoMap{}
	temp.FileInfoMap = m.FileMetaMap
	return &temp, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// panic("todo")
	temp := Version{}
	temp.Version = -1
	key := fileMetaData.Filename
	newVersion := fileMetaData.Version
	if metadata, ok := m.FileMetaMap[key]; ok { // file exists
		if metadata.Version + 1 == newVersion { // ok to update
			m.FileMetaMap[key] = fileMetaData
			temp.Version = newVersion
		}
	} else { // file does not exist
		m.FileMetaMap[key] = fileMetaData
		temp.Version = newVersion
	}
	fmt.Println(m.FileMetaMap[key])
	return &temp, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	// panic("todo")
	temp := BlockStoreAddr{}
	temp.Addr = m.BlockStoreAddr
	return &temp, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
