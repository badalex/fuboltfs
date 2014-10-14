package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}

	you, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}

	myfs, err := newfs(you.HomeDir + "/fstorage")
	if err != nil {
		log.Fatal(err)
	}
	defer myfs.CloseBolt()

	fs.Serve(c, myfs)

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

