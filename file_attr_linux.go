package main

import (
	"bazil.org/fuse"
	"log"
	"strconv"
	"syscall"
	"time"
)

var _ = log.Println

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

		attr.Atime = time.Unix(stat.Atim.Unix())
		attr.Mtime = time.Unix(stat.Mtim.Unix())
		attr.Ctime = time.Unix(stat.Ctim.Unix())

		attr.Nlink = uint32(stat.Nlink)
	}

	return attr
}

