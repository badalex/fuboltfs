package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

func (myfs *FS) Root() (fs.Node, fuse.Error) {
	r := Dir{inode: root_inode, fs: myfs}
	return r, nil
}
