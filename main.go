package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"strconv"

	"sigint.ca/fuse"
)

var (
	serverUid uint32
	serverGid uint32
)

var (
	debug = flag.Bool("d", false, "debug")
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage: %s mountpoint bucket\n", os.Args[0])
	flag.PrintDefaults()
}

func init() {
	u, err := user.Current()
	if err != nil {
		log.Fatalf("failed to lookup current user: %v", err)
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		log.Fatalf("failed to parse uid: %v", err)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		log.Fatalf("failed to parse gid: %v", err)
	}
	serverUid = uint32(uid)
	serverGid = uint32(gid)
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 2 {
		Usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)
	bucketname := flag.Arg(1)

	s3Init(bucketname)

	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	c.Serve(s3fs{bucket: bucketname})
}

func dprintf(format string, args ...interface{}) {
	if *debug {
		fmt.Fprintf(os.Stderr, "[ s3fs: "+format+" ]\n", args...)
	}
}
