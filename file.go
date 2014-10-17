package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/boltdb/bolt"
	"errors"
	"log"
	"strconv"
	"syscall"
	"time"
)

var _ = log.Println

type File struct {
	inode uint64
	fs *FS
}

func (f File) Attr() fuse.Attr {
	log.Println(f.inode, "fattr")

	fpath := f.fs.storagepath + "/files/" + strconv.FormatUint(f.inode, 10)

	attr := fuse.Attr{
		Inode: f.inode,
		Mode: 0644,
		Nlink: 1,
	}

	stat := syscall.Stat_t{}
	err := syscall.Stat(fpath, &stat)
	if err == nil {
		log.Printf("%+v\n", stat)

		attr.Size = uint64(stat.Size)
		attr.Blocks = uint64(stat.Blocks)

		attr.Atime = time.Unix(stat.Atimespec.Unix())
		attr.Mtime = time.Unix(stat.Mtimespec.Unix())
		attr.Ctime = time.Unix(stat.Ctimespec.Unix())

		attr.Nlink = uint32(stat.Nlink)
	}

	return attr
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


func (f File) Listxattr(req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse, intr fs.Intr) fuse.Error {
	return f.fs.db.View(func(tx *bolt.Tx) error {
		xtb := tx.Bucket([]byte("xattrs"))
		if xtb == nil {
			return errors.New("Missing xattrs bucket")
		}
		key := uint64_b(f.inode)
		xb := xtb.Bucket(key)
		if xb == nil {
			// no xattrs for this file
			return nil
		}
		xb.ForEach(func(k, v []byte) error {
			resp.Append(string(k))
			return nil
		})
		return nil
	})
}

func (f File) Getxattr(req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse, intr fs.Intr) fuse.Error {
	return f.fs.db.View(func(tx *bolt.Tx) error {
		xtb := tx.Bucket([]byte("xattrs"))
		if xtb == nil {
			return errors.New("Missing xattrs bucket")
		}
		key := uint64_b(f.inode)
		xb := xtb.Bucket(key)
		if xb == nil {
			// no xattrs for this file
			return nil
		}
		resp.Xattr = xb.Get([]byte(req.Name))
		return nil
	})
}

func (f File) Setxattr(req *fuse.SetxattrRequest, intr fs.Intr) fuse.Error {
	return f.fs.db.Update(func(tx *bolt.Tx) error {
		xtb := tx.Bucket([]byte("xattrs"))
		if xtb == nil {
			return errors.New("Missing xattrs bucket")
		}
		key := uint64_b(f.inode)
		xb, err := xtb.CreateBucketIfNotExists(key)
		if err != nil {
			return err
		}
		return xb.Put([]byte(req.Name), req.Xattr)
	})
}

func (f File) Removexattr(req *fuse.RemovexattrRequest, intr fs.Intr) fuse.Error {
	return f.fs.db.Update(func(tx *bolt.Tx) error {
		xtb := tx.Bucket([]byte("xattrs"))
		if xtb == nil {
			return errors.New("Missing xattrs bucket")
		}
		key := uint64_b(f.inode)
		xb := xtb.Bucket(key)
		if xb == nil {
			return nil
		}
		return xb.Delete([]byte(req.Name))
	})
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
