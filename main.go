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

func dbg(msg interface{}) {
	log.Println(msg)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return false
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}

	you, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}

	if !exists(you.HomeDir + "/fstorage") {
		err := os.Mkdir(you.HomeDir + "/fstorage", 0700)
		if err != nil {
			log.Fatal(err)
		}
	}
	if !exists(you.HomeDir + "/fstorage/files") {
		err := os.Mkdir(you.HomeDir + "/fstorage/files", 0700)
		if err != nil {
			log.Fatal(err)
		}
	}

	myfs, err := newfs(you.HomeDir + "/fstorage")
	if err != nil {
		log.Fatal(err)
	}
	defer myfs.CloseBolt()

	myfs.DatabaseIDPrompt()

	err = myfs.SpawnAdminConsole()
	if err != nil {
		log.Fatal(err)
	}


	mountpoint := flag.Arg(0)

	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("fs ready, dbid", myfs.dbid, "(admin console: nc localhost 2000)")

	server := fs.Server{
		FS: myfs,
//		Debug: dbg,
	}
	server.Serve(c)

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}

	log.Println("fs shut down nicely")
}

