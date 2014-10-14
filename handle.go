package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
//	"github.com/boltdb/bolt"
	"os"
	"strconv"
	"log"
)

type Handle struct {
	file *File
	fh *os.File
	oflags fuse.OpenFlags
}

func NewHandle(file *File, oflags fuse.OpenFlags) (*Handle, error) {
	fpath := file.fs.storagepath + "/files/" + strconv.FormatUint(file.inode, 10)

	h := Handle{
		file: file,
		oflags: oflags,
	}

	var err error
	h.fh, err = os.OpenFile(fpath, int(oflags), 0644)
	if err != nil {
		return nil, err
	}

	return &h, nil
}

func (h *Handle) Read(req *fuse.ReadRequest, resp *fuse.ReadResponse, intr fs.Intr) fuse.Error {
	return nil
}

func (h *Handle) Write(req *fuse.WriteRequest, resp *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	return nil
}

func (h *Handle) Release(req *fuse.ReleaseRequest, intr fs.Intr) fuse.Error {
	if h.fh != nil {

		stat, err := h.fh.Stat()
		if err != nil {
			return err
		}

		log.Println("closed filehandle, inode", h.file.inode, "size", h.file.fsize, "stat", stat)
		return h.fh.Close()
	}
	log.Println("closed handle with no open filehandle")
	return nil
}
