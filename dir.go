package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"os"
	"errors"
	"syscall"
	"github.com/boltdb/bolt"
	"log"
	"fmt"
)

var _ = log.Printf
var _ = fmt.Println

type Dir struct {
	inode uint64
	fs *FS
}

func (d Dir) Attr() fuse.Attr {
	//(d.inode, "dattr")
	return fuse.Attr{Inode: d.inode, Mode: os.ModeDir | 0555}
}

func (d Dir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	//log.Println(d.inode, "lookup", name)

	var r fs.Node

	err := d.fs.db.View(func(tx *bolt.Tx) error {
		fsizes := tx.Bucket([]byte("filesize"))
		if fsizes == nil {
			return errors.New("Missing filesize bucket")
		}
		kids := tx.Bucket([]byte("kids"))
		if kids == nil {
			return errors.New("Missing kids bucket")
		}
		dkids := kids.Bucket(uint64_b(d.inode))
		if dkids == nil {
			return errors.New("Missing directory kids bucket")
		}
		match := dkids.Get([]byte(name))
		if match == nil {
			return fuse.ENOENT
		}
		inode := b_uint64(match)
		if inode == 0 {
			return fuse.ENOENT
		}
		fsizev := fsizes.Get(match)
		//log.Println(inode, "lookup size", b_uint64(fsizev))
		if fsizev == nil {
			r = Dir{inode: inode, fs: d.fs}
		} else {
			r = File{inode: inode, fs: d.fs}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (d Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	//log.Println(d.inode, "readdir")

	list := []fuse.Dirent{}

	err := d.fs.db.View(func(tx *bolt.Tx) error {
		fsizes := tx.Bucket([]byte("filesize"))
		if fsizes == nil {
			return errors.New("Missing filesize bucket")
		}
		kids := tx.Bucket([]byte("kids"))
		if kids == nil {
			return errors.New("Missing kids bucket")
		}
		dkids := kids.Bucket(uint64_b(d.inode))
		if dkids == nil {
			return errors.New("Missing directory kids bucket")
		}
		dkids.ForEach(func(k, v []byte) error {
			name := string(k)
			inode := b_uint64(v)

			fsize := fsizes.Get(v)
			typ := fuse.DT_Dir
			if fsize != nil {
				typ = fuse.DT_File
			}

			//log.Println(inode, "dirent size", b_uint64(fsize))
			list = append(list, fuse.Dirent{
				Inode: inode,
				Name: name,
				Type: typ,
			})
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	return list, nil
}


func (d Dir) Rename(req *fuse.RenameRequest, newDir fs.Node, intr fs.Intr) fuse.Error {
	//log.Println(d.inode, "rename")

	if req.NewName == req.OldName && newDir.Attr().Inode == d.inode {
		// seems to be a noop
		return nil
	}

	return d.fs.db.Update(func(tx *bolt.Tx) error {
		kids := tx.Bucket([]byte("kids"))
		if kids == nil {
			return errors.New("Missing kids bucket")
		}
		dkids := kids.Bucket(uint64_b(d.inode))
		if dkids == nil {
			return errors.New("Missing directory kids bucket")
		}
		key := []byte(req.OldName)
		exists := dkids.Get(key)
		if exists == nil {
			return fuse.Errno(syscall.ENOENT)
		}

		// put it into the new folder before we remove it from the old one
		new_dir_inode := newDir.Attr().Inode

		var ndkids *bolt.Bucket
		if new_dir_inode == d.inode {
			ndkids = dkids
		} else {
			ndkids = kids.Bucket(uint64_b(new_dir_inode))
			if ndkids == nil {
				return errors.New("Missing new directory kids bucket")
			}
		}

		newkey := []byte(req.NewName)

		err := ndkids.Put(newkey, exists)
		if err != nil {
			return err
		}

		err = dkids.Delete(key)
		if err != nil {
			return err
		}

		//inode := b_uint64(exists)
		//log.Println(inode, "moved from", d.inode, "to", new_dir_inode, "name from", req.OldName, "to", req.NewName)

		return nil
	})
}

func (d Dir) Remove(req *fuse.RemoveRequest, intr fs.Intr) fuse.Error {
	//log.Println(d.inode, "remove", req.Name)

	return d.fs.db.Update(func(tx *bolt.Tx) error {
		kids := tx.Bucket([]byte("kids"))
		if kids == nil {
			return errors.New("Missing kids bucket")
		}
		dkids := kids.Bucket(uint64_b(d.inode))
		if dkids == nil {
			return errors.New("Missing directory kids bucket")
		}
		key := []byte(req.Name)
		exists := dkids.Get(key)
		if exists == nil {
			return fuse.Errno(syscall.ENOENT)
		}
		//inode := b_uint64(exists)
		//log.Println(inode, "removed")
		return dkids.Delete(key)
	})
}

func (d Dir) Mkdir(req *fuse.MkdirRequest, intr fs.Intr) (fs.Node, fuse.Error) {
	//log.Println(d.inode, "mkdir", req.Name)

	var child fs.Node
	err := d.fs.db.Update(func(tx *bolt.Tx) error {
		kids := tx.Bucket([]byte("kids"))
		if kids == nil {
			return errors.New("Missing kids bucket")
		}
		dkids := kids.Bucket(uint64_b(d.inode))
		if dkids == nil {
			return errors.New("Missing directory kids bucket")
		}
		key := []byte(req.Name)
		exists := dkids.Get(key)
		if exists != nil {
			return fuse.Errno(syscall.EEXIST)
		}

		inode, err := d.fs.NewInode(tx)
		if err != nil {
			return err
		}

		val := uint64_b(inode)
		err = dkids.Put(key, val)
		if err != nil {
			return err
		}
		_, err = kids.CreateBucket(val)
		if err != nil {
			return err
		}

		txn, err := d.fs.NewTx(tx, TX_MKDIR, d.inode, key, 0, nil)
		if err != nil {
			return err
		}
		_ = txn

		child = &Dir{inode: inode, fs: d.fs}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return child, err
}


func (d Dir) Create(req *fuse.CreateRequest, resp *fuse.CreateResponse, intr fs.Intr) (fs.Node, fs.Handle, fuse.Error) {
	//log.Println(d.inode, "create")

	//log.Println("create request, flags", req.Flags, "mode", req.Mode)

	var child fs.Node
	var handle fs.Handle
	err := d.fs.db.Update(func(tx *bolt.Tx) error {
		fsizes := tx.Bucket([]byte("filesize"))
		if fsizes == nil {
			return errors.New("Missing filesize bucket")
		}
		kids := tx.Bucket([]byte("kids"))
		if kids == nil {
			return errors.New("Missing kids bucket")
		}
		dkids := kids.Bucket(uint64_b(d.inode))
		if dkids == nil {
			return errors.New("Missing directory kids bucket")
		}
		key := []byte(req.Name)
		exists := dkids.Get(key)
		if exists != nil {
			return fuse.Errno(syscall.EEXIST)
		}

		inode, err := d.fs.NewInode(tx)
		if err != nil {
			return err
		}

		val := uint64_b(inode)
		dkids.Put(key, val)
		fsizes.Put(val, uint64_b(0))

		//log.Println(inode, "created")

		newfile := File{inode: inode, fs: d.fs}
		child = &newfile

		handle, err = NewHandle(&newfile, req.Flags)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, nil, err
	}
	return child, handle, err
}

func (d Dir) Listxattr(req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse, intr fs.Intr) fuse.Error {
	f := File{inode: d.inode, fs: d.fs}
	return f.Listxattr(req, resp, intr)
}

func (d Dir) Getxattr(req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse, intr fs.Intr) fuse.Error {
	f := File{inode: d.inode, fs: d.fs}
	return f.Getxattr(req, resp, intr)
}

func (d Dir) Setxattr(req *fuse.SetxattrRequest, intr fs.Intr) fuse.Error {
	f := File{inode: d.inode, fs: d.fs}
	return f.Setxattr(req, intr)
}

func (d Dir) Removexattr(req *fuse.RemovexattrRequest, intr fs.Intr) fuse.Error {
	f := File{inode: d.inode, fs: d.fs}
	return f.Removexattr(req, intr)
}


