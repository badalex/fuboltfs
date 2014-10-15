package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/boltdb/bolt"
	"errors"
	"log"
)

var _ = log.Println

type File struct {
	inode uint64
	fs *FS
}

func (f File) Attr() fuse.Attr {
	log.Println(f.inode, "fattr")
	return fuse.Attr{
		Inode: f.inode,
		Mode: 0644,
		Size: f.LoadSize(),
		Nlink: 1,
	}
}

func (f File) Fsync(req *fuse.FsyncRequest, intr fs.Intr) fuse.Error {
	log.Println(f.inode, "sync")

	// TODO: implement this when bazil.org/fuse moves it from Node to Handle
	// this data structure does not have the open filehandle so I don't know
	// how to call Sync() on it :(
	//
	// Well at least flush works.  Hopefully that's not scary. :D
	return nil
}

func (f File) LoadSize() uint64 {
	var fsize uint64
	f.fs.db.View(func(tx *bolt.Tx) error {
		fsizes := tx.Bucket([]byte("filesize"))
		if fsizes == nil {
			return errors.New("Missing filesize bucket")
		}
		key := uint64_b(f.inode)
		val := fsizes.Get(key)
		if val == nil {
			return errors.New("Filesize missing")
		}
		fsize = b_uint64(val)
		return nil
	})
	return fsize
}

func (f File) Open(req *fuse.OpenRequest, resp *fuse.OpenResponse, intr fs.Intr) (fs.Handle, fuse.Error) {
	log.Println(f.inode, "open")
	return NewHandle(&f, req.Flags)
}

func (f File) SaveSize(size uint64) error {

	err := f.fs.db.Update(func(tx *bolt.Tx) error {
		fsizes := tx.Bucket([]byte("filesize"))
		if fsizes == nil {
			return errors.New("Missing filesize bucket")
		}
		key := uint64_b(f.inode)
		val := fsizes.Get(key)
		if val == nil {
			return errors.New("File size key missing, cannot update")
		}
		val = uint64_b(size)
		return fsizes.Put(key, val)
	})

	return err
}
