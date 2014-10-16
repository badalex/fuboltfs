package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
//	"github.com/boltdb/bolt"
	"os"
	"io"
	"strconv"
	"log"
	"sync"
	"syscall"
	"strings"
)

var _ = log.Println
var _ = strings.Trim

type Handle struct {
	file *File
	fh *os.File
	oflags fuse.OpenFlags
	id int
	lastoffset int64
}

var hid int
var hidmu sync.RWMutex

func newhid() int {
	hidmu.Lock()
	defer hidmu.Unlock()
	hid++
	return hid
}

func NewHandle(file *File, oflags fuse.OpenFlags) (*Handle, error) {
	fpath := file.fs.storagepath + "/files/" + strconv.FormatUint(file.inode, 10)

	h := Handle{
		file: file,
		oflags: oflags,
		id: newhid(),
		lastoffset: 0,
	}

	var err error
	h.fh, err = os.OpenFile(fpath, int(oflags), 0600)
	if err != nil {
		return nil, err
	}

	log.Println(h.file.inode, "handle", h.id, "oflags", oflags)

	return &h, nil
}

func (h *Handle) Flush(req *fuse.FlushRequest, intr fs.Intr) fuse.Error {
	log.Println(h.file.inode, "handle", h.id, "flush")
	err := h.fh.Sync()
	if err != nil {
		return err
	}

	if h.oflags & syscall.O_RDONLY == 0 {
		stat, err := h.fh.Stat()
		if err != nil {
			return err
		}

		s := uint64(stat.Size())
		old := h.file.LoadSize()

		if s != old {
			log.Println(h.file.inode, "handle", h.id, "resize", s, "from", old)

			err = h.file.SaveSize(s)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Handle) Read(req *fuse.ReadRequest, resp *fuse.ReadResponse, intr fs.Intr) fuse.Error {
	var n int
	var err error
	buf := resp.Data[:req.Size]

	if req.Offset == h.lastoffset {
		n, err = h.fh.Read(buf)
		h.lastoffset += int64(n)
	} else {
		n, err = h.fh.ReadAt(buf, req.Offset)
	}

	resp.Data = buf[:n]
	//log.Println(h.file.inode, "handle", h.id, "read", strings.Trim(string(resp.Data), "\n"))
	if n == 0 && err == io.EOF {
		return io.EOF
	}
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

func (h *Handle) Write(req *fuse.WriteRequest, resp *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	n, err := h.fh.WriteAt(req.Data, req.Offset)
	resp.Size = n

	//dn := n
	//if dn > 30 {
	//	dn = 30
	//}
	//dbg := strings.Replace(string(req.Data[:dn]), "\n", " ", -1)
	//log.Println(h.file.inode, "handle", h.id, "writ[", dbg, "]", n)
	return err
}

func (h *Handle) Release(req *fuse.ReleaseRequest, intr fs.Intr) fuse.Error {
	log.Println(h.file.inode, "handle", h.id, "released")

	if h.fh != nil {
		if h.oflags & syscall.O_RDONLY == 0 {
			stat, err := h.fh.Stat()
			if err != nil {
				h.fh.Close()
				return err
			}

			s := uint64(stat.Size())
			old := h.file.LoadSize()

			if s != old {
				log.Println(h.file.inode, "handle", h.id, "resize", s, "from", old)

				err = h.file.SaveSize(s)
				if err != nil {
					h.fh.Close()
					return err
				}
			}
		}

		return h.fh.Close()
	}
	return nil
}
