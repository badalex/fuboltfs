package main

import (
	"github.com/boltdb/bolt"
	"encoding/binary"
	"bytes"
	"errors"
	"sync"
)

const root_inode uint64 = 1
const min_inode uint64 = 1024

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

type FS struct {
	storagepath string
	db	*bolt.DB
	seqmu sync.RWMutex
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
	fs.seqmu.Lock()
	defer fs.seqmu.Unlock()

	b := tx.Bucket([]byte("misc"))
	if b == nil {
		return 0, errors.New("Misc bucket not found")
	}
	r := b_uint64(b.Get([]byte("lastinode")))
	if r < min_inode {
		r = min_inode
	}
	r++
	b.Put([]byte("lastinode"), uint64_b(r))
	return r, nil
}

