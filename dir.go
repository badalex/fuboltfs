package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"os"
	"errors"
	"syscall"
	"github.com/boltdb/bolt"
)

type Dir struct {
	inode uint64
	fs *FS
}

func (d Dir) Attr() fuse.Attr {
	return fuse.Attr{Inode: d.inode, Mode: os.ModeDir | 0555}
}

func (d Dir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {

	var r fs.Node

	err := d.fs.db.View(func(tx *bolt.Tx) error {
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
		r = Dir{inode: inode, fs: d.fs}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (d Dir) Rename(req *fuse.RenameRequest, newDir fs.Node, intr fs.Intr) fuse.Error {

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

		return nil
	})
}

func (d Dir) Remove(req *fuse.RemoveRequest, intr fs.Intr) fuse.Error {
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
		return dkids.Delete(key)
	})
}

func (d Dir) Mkdir(req *fuse.MkdirRequest, intr fs.Intr) (fs.Node, fuse.Error) {
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
		dkids.Put(key, val)
		kids.CreateBucket(val)

		child = &Dir{inode: inode, fs: d.fs}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return child, err
}

func (d Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	list := []fuse.Dirent{}

	err := d.fs.db.View(func(tx *bolt.Tx) error {
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

			list = append(list, fuse.Dirent{
				Inode: inode,
				Name: name,
				Type: fuse.DT_Dir,
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

