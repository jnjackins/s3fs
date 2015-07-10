package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"sigint.ca/fuse"
)

var (
	debug = flag.Bool("d", false, "debug")
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage: %s mountpoint bucket\n", os.Args[0])
	flag.PrintDefaults()
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
