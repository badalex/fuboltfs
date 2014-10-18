package main

import (
	"github.com/boltdb/bolt"
	"encoding/binary"
	"bytes"
	"errors"
	"sync"
	"log"
	"syscall"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"fmt"
	"strconv"
)

var _ = log.Println

const root_inode uint64 = 1
const min_inode uint64 = 10

const max_name_len = 4096
const max_txn_size = max_name_len*2 + 256


type FS struct {
	storagepath string
	db	*bolt.DB
	seqmu sync.RWMutex
	generation uint64
	dbid uint16
	txid uint64
}

func newfs(stoarage string) (*FS, error) {
	db, err := bolt.Open(stoarage + "/fs.bolt", 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("misc")); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("filesize")); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("xattrs")); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("tx")); err != nil {
			return err
		}
		cb, err := tx.CreateBucketIfNotExists([]byte("kids"))
		if err != nil {
			return err
		}
		if _, err := cb.CreateBucketIfNotExists(uint64_b(root_inode)); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	fs := FS{
		storagepath: stoarage,
		db: db,
	}

	return &fs, nil
}

func (fs *FS) CloseBolt() {
	fs.db.Close()
}

func (fs *FS) NewInode(tx *bolt.Tx) (uint64, error) {
	b := tx.Bucket([]byte("misc"))
	if b == nil {
		return 0, errors.New("Misc bucket not found")
	}
	r := b_uint64(b.Get([]byte("lastinode")))
	if r < min_inode {
		r = min_inode
	}
	r++
	err := b.Put([]byte("lastinode"), uint64_b(r))
	if err != nil {
		return 0, err
	}
	return r, nil
}

func (fs *FS) NewTxId(tx *bolt.Tx) (uint64, error) {
	b := tx.Bucket([]byte("misc"))
	if b == nil {
		return 0, errors.New("Misc bucket not found")
	}
	r := b_uint64(b.Get([]byte("lasttxid")))
	r++
	err := b.Put([]byte("lasttxid"), uint64_b(r))
	if err != nil {
		return 0, err
	}
	return r, nil
}

func (fs *FS) Generation() uint64 {
	fs.seqmu.RLock()
	defer fs.seqmu.RUnlock()
	return fs.generation
}

func (fs *FS) NewGeneration() uint64 {
	fs.seqmu.Lock()
	defer fs.seqmu.Unlock()
	fs.generation++
	return fs.generation
}

func (fs *FS) Statfs(req *fuse.StatfsRequest, resp *fuse.StatfsResponse, intr fs.Intr) fuse.Error {
	// statfs our storage directory
	stat := syscall.Statfs_t{}
	err := syscall.Statfs(fs.storagepath, &stat)
	if err != nil {
		return err
	}

	resp.Blocks = stat.Blocks
	resp.Bfree = stat.Bfree
	resp.Bavail = stat.Bavail
	resp.Files = stat.Files
	resp.Ffree = stat.Ffree
	resp.Bsize = uint32(stat.Bsize)
	resp.Namelen = 4096
	resp.Frsize = 4096
	// maybe on linux we pass it through?
	//uint32(stat.Frsize)
	return nil
}

func (fs *FS) DatabaseIDPrompt() {
	fs.seqmu.RLock()
	defer fs.seqmu.RUnlock()

	err := fs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("misc"))
		if b == nil {
			return errors.New("Misc bucket not found")
		}
		r := b_uint64(b.Get([]byte("database_id")))
		for (r == 0 || r > 65535) {
			fmt.Println("FIRST TIME SETUP")
			fmt.Println("Enter your database ID (this must be unique over the cluster! typo will cause problems, so be careful)")
			fmt.Println("Later these will be auto-assigned from the cluster master node but for now, ask Joel for one.")
			fmt.Println("CTRL+C to abort")
			fmt.Print("> ")
			str := ""
			n, err := fmt.Scanf("%s", &str)
			if err != nil || n != 1 {
				r = 0
			} else {
				i, err := strconv.ParseUint(str, 10, 64)
				if err != nil {
					fmt.Println(err)
					r = 0
				} else {
					r = i
				}
			}
		}
		fs.dbid = uint16(r)
		return b.Put([]byte("database_id"), uint64_b(r))
	})

	if err != nil {
		log.Fatal(err)
	}
}




func uint16_b(i uint16) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, i)
	return buf.Bytes()
}
func uint64_b(i uint64) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, i)
	return buf.Bytes()
}
func b_uint64(b []byte) uint64 {
	if(len(b) != 8) {
		return 0
	}
	var i uint64
	buf := bytes.NewReader(b)
	binary.Read(buf, binary.LittleEndian, &i)
	return i
}
