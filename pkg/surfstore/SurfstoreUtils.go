package surfstore

import (
	"log"
	"fmt"
	"io/ioutil"
	"os"
	"io"
	"errors"
	"reflect"
	//"strconv"
	//grpc "google.golang.org/grpc"
	// "context"
	// "google.golang.org/protobuf/types/known/emptypb"
	// "time"
)

func GenerateIndex(client *RPCClient, m map[string]*FileMetaData) map[string]*FileMetaData {
	m2 := make(map[string]*FileMetaData)
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}
	
	for _, file := range files {
		fmt.Println(file.Name())
		if file.Name() != DEFAULT_META_FILENAME {
			// get block hashlist for this file
			f, err := os.Open(ConcatPath(client.BaseDir, file.Name()))
			if err != nil {
				log.Fatal(err)
			}
			

			hashList := make([]string, 0)
			buf := make([]byte, client.BlockSize)
			for {
				_, err1 := f.Read(buf)
				if err1 != nil {
					if err1 != io.EOF {
						log.Fatal(err1)
					}
					break
				}
				hashList = append(hashList, GetBlockHashString(buf))
			}
			f.Close()

			temp := FileMetaData{
				Filename : file.Name(),
				Version : 1,
				BlockHashList : hashList,
			}
			m2[file.Name()] = &temp
		}
	}
	WriteMetaFile(m2, client.BaseDir)
	return m2
}

func UpdateIndex(client *RPCClient, m map[string]*FileMetaData) map[string]*FileMetaData {
	m2 := make(map[string]*FileMetaData)
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}
	
	for _, file := range files {
		if file.Name() != DEFAULT_META_FILENAME {
			fmt.Println(file.Name())

			// get block hashlist for this file
			f, err := os.Open(ConcatPath(client.BaseDir, file.Name()))
			if err != nil {
				log.Fatal(err)
			}
			

			hashList := make([]string, 0)
			buf := make([]byte, client.BlockSize)
			for {
				_, err1 := f.Read(buf)
				if err1 != nil {
					if err1 != io.EOF {
						log.Fatal(err1)
					}
					break
				}
				hashList = append(hashList, GetBlockHashString(buf))
			}
			f.Close()

			temp := FileMetaData{}
			temp.Filename = file.Name()
			if _, ok := (m)[file.Name()]; ok { // entry exists in index.txt
				if reflect.DeepEqual((m)[file.Name()].BlockHashList, hashList) { // no mods, version stay same
					temp.Version = (m)[file.Name()].Version
				} else {
					temp.Version = (m)[file.Name()].Version + 1
				}
			}
			temp.BlockHashList = hashList
			m2[file.Name()] = &temp
		}
	}
	return m2
}

func Upload(client *RPCClient, m map[string]*FileMetaData, blockStoreAddr string) {
	// don't make change if client crashed

	var mClient map[string]*FileMetaData
	client.GetFileInfoMap(&mClient)

	for file, metadata := range m {
		var latestVersion int32
		client.UpdateFile(metadata, &latestVersion) // only new versions updated
		//println("updating file")
		if latestVersion != -1 { // we made update, so need to update blocks
			// put blocks
			if _, err := os.Stat(ConcatPath(client.BaseDir, file)); errors.Is(err, os.ErrNotExist) { // file deleted
				// do nothing
			} else {
				f, err := os.Open(ConcatPath(client.BaseDir, file))
				if err != nil {
					log.Fatal(err)
				}
				

				buf := make([]byte, client.BlockSize)
				for {
					bytes, err1 := f.Read(buf)
					if err1 != nil {
						if err1 != io.EOF {
							log.Fatal(err1)
						}
						break
					}
					temp := Block {
						BlockData: buf,
						BlockSize: int32(bytes),
					}			
					var succ bool
					client.PutBlock(&temp, blockStoreAddr, &succ)
				}
				f.Close()
			}
		}
	}
}

// update blocks
func Download(client *RPCClient, blockStoreAddr string) {
	// don't make change if client crashed

	var mClient map[string]*FileMetaData
	client.GetFileInfoMap(&mClient)
	//PrintMetaMap(mClient)
	for file, metadata := range mClient {

		// if file exists but server says its deleted, then remove
		if len(metadata.BlockHashList) == 1 && metadata.BlockHashList[0] == "0" {
			if _, err := os.Stat(ConcatPath(client.BaseDir, file)); err == nil { // file exists
				os.Remove(ConcatPath(client.BaseDir, file))	
			}
		} else { // server says file not deleted

			f, err := os.Create(ConcatPath(client.BaseDir, file))
			if err != nil {
				log.Fatal(err)
			}
			for _, val := range metadata.BlockHashList {
				temp := Block{}
				client.GetBlock(val, blockStoreAddr, &temp)
				f.Write(temp.BlockData[:temp.BlockSize])
				//println(string(temp.BlockData))
				//println(string(temp.BlockData[:temp.BlockSize]))
				println(temp.BlockSize)
			}
			f.Close()
		}
	}
}



// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// panic("todo")

	// don't do anything if client server crashed
	// server,_ := strconv.Atoi(client.BaseDir[4:])
	// conn1, err1 := grpc.Dial(client.MetaStoreAddrs[server], grpc.WithInsecure())
	// if err1 != nil {
	// 	return
	// }
	// c1 := NewRaftSurfstoreClient(conn1)
	// ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
	// defer cancel1()
	// if state, _ := c1.IsCrashed(ctx1, &emptypb.Empty{}); state.IsCrashed {
	// 	return // don't sync if client is crashed
	// }


	// get local file meta map
	//m := make(map[string]*FileMetaData)
	m2 := make(map[string]*FileMetaData)
	if _, err := os.Stat(ConcatPath(client.BaseDir, DEFAULT_META_FILENAME)); errors.Is(err, os.ErrNotExist) {// no index.txt
		//m = GenerateIndex(&client, m)
		files, err := ioutil.ReadDir(client.BaseDir)
		if err != nil {
			log.Fatal(err)
		}
		
		for _, file := range files {
			if file.Name() != DEFAULT_META_FILENAME {
				// get block hashlist for this file
				fmt.Println(file.Name())
				f, err := os.Open(ConcatPath(client.BaseDir, file.Name()))
				if err != nil {
					log.Fatal(err)
				}

				hashList := make([]string, 0)
				buf := make([]byte, client.BlockSize)
				for {
					_, err1 := f.Read(buf)
					if err1 != nil {
						if err1 != io.EOF {
							log.Fatal(err1)
						}
						break
					}
					hashList = append(hashList, GetBlockHashString(buf))
				}
				f.Close()

				temp := FileMetaData{
					Filename : file.Name(),
					Version : 1,
					BlockHashList : hashList,
				}
				m2[file.Name()] = &temp
			}
		}
		//WriteMetaFile(m2, client.BaseDir)
	} else { // load file meta map from index.txt
		m, _ := LoadMetaFromMetaFile(client.BaseDir)
		//m = UpdateIndex(&client, m)
		
		if err != nil {
			log.Fatal(err)
		}
		
		for file, _ := range m {
			if file != DEFAULT_META_FILENAME {
				fmt.Println(file)

				hashList := make([]string, 0)
				if _, err := os.Stat(ConcatPath(client.BaseDir, file)); errors.Is(err, os.ErrNotExist) { // file deleted
					hashList = append(hashList, "0")
				} else {
					// get block hashlist for this file
					f, err := os.Open(ConcatPath(client.BaseDir, file))
					if err != nil {
						log.Fatal(err)
					}
					
					
					buf := make([]byte, client.BlockSize)
					for {
						_, err1 := f.Read(buf)
						if err1 != nil {
							if err1 != io.EOF {
								log.Fatal(err1)
							}
							break
						}
						hashList = append(hashList, GetBlockHashString(buf))
					}
					f.Close()
				}

				temp := FileMetaData{}
				temp.Filename = file
				if _, ok := (m)[file]; ok { // entry exists in index.txt
					if reflect.DeepEqual((m)[file].BlockHashList, hashList) { // no mods, version stay same
						temp.Version = (m)[file].Version
					} else {
						temp.Version = (m)[file].Version + 1
					}
				}
				
				temp.BlockHashList = hashList
				m2[file] = &temp
			}

		}

		// add created files
		files, _ := ioutil.ReadDir(client.BaseDir)
		for _, fi := range files {
			file := fi.Name()
			if _, ok := m2[file]; ok==false { // new entry
				if file != DEFAULT_META_FILENAME {
					fmt.Println(file)
					// get block hashlist for this file
					f, err := os.Open(ConcatPath(client.BaseDir, file))
					if err != nil {
						log.Fatal(err)
					}
					
					hashList := make([]string, 0)
					buf := make([]byte, client.BlockSize)
					for {
						_, err1 := f.Read(buf)
						if err1 != nil {
							if err1 != io.EOF {
								log.Fatal(err1)
							}
							break
						}
						hashList = append(hashList, GetBlockHashString(buf))
					}
					f.Close()

					temp := FileMetaData{}
					temp.Filename = file
					temp.Version = 1
					
					temp.BlockHashList = hashList
					m2[file] = &temp

				}
			}
		}
	}

	var blockStoreAddr string
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
		log.Fatal(err)
	}

	// upload what can be uploaded
	Upload(&client, m2, blockStoreAddr)
	
	// always download everything and discard local changes
	Download(&client, blockStoreAddr)

	// update index.txt
	os.Remove(ConcatPath(client.BaseDir, DEFAULT_META_FILENAME))
	var mClient map[string]*FileMetaData
	client.GetFileInfoMap(&mClient)
	WriteMetaFile(mClient, client.BaseDir)
	
	fmt.Println("sync complete")

	// verify we got block we want
	//log.Print(block.String())
}
