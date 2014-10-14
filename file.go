package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
//	"github.com/boltdb/bolt"
)

type File struct {
	inode uint64
	fs *FS
	fsize uint64
}

func (f File) Attr() fuse.Attr {
	return fuse.Attr{
		Inode: f.inode,
		Mode: 0644,
		Size: f.fsize,
	}
}

func (f File) Open(req *fuse.OpenRequest, resp *fuse.OpenResponse, intr fs.Intr) (fs.Handle, fuse.Error) {
	return NewHandle(&f, req.Flags)
}

//func (File) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
//	return []byte(greeting), nil
//}

