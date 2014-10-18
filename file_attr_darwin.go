package main

import (
	"bazil.org/fuse"
	"syscall"
	"time"
)

func bazil_attr_from_stat_t(stat *syscall.Stat_t, attr *fuse.Attr) {
	attr.Size = uint64(stat.Size)
	attr.Blocks = uint64(stat.Blocks)

	attr.Atime = time.Unix(stat.Atimespec.Unix())
	attr.Mtime = time.Unix(stat.Mtimespec.Unix())
	attr.Ctime = time.Unix(stat.Ctimespec.Unix())

	attr.Nlink = uint32(stat.Nlink)
}
