package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"sigint.ca/fuse"
)

const (
	tUnknown = iota
	tPrefix
	tObject
)

type s3fs struct {
	bucket string
	key    string
	kind   int
	size   uint64
	body   io.ReadCloser
}

func (ctx s3fs) String() string {
	var kind string
	switch ctx.kind {
	case tUnknown:
		kind = "unknown"
	case tPrefix:
		kind = "prefix"
	case tObject:
		kind = "object"
	}
	return fmt.Sprintf("{key: %#v, kind: %s}", ctx.key, kind)
}

func (ctx s3fs) Root() (fuse.Node, fuse.Error) {
	dprintf("Root: %v", ctx)
	ctx.key = ""
	ctx.kind = tPrefix
	return ctx, nil
}

func (ctx s3fs) Attr() fuse.Attr {
	dprintf("Attr: %v", ctx)
	switch ctx.kind {
	case tPrefix:
		dprintf("Attr: type is directory")
		return fuse.Attr{Mode: os.ModeDir | 0555}
	case tObject:
		dprintf("Attr: type is regular")
		return fuse.Attr{Mode: 0444, Size: ctx.size}
	default:
		dprintf("Attr: type is unsupported")
		return fuse.Attr{}
	}
}

func (ctx s3fs) Lookup(name string, intr fuse.Intr) (fuse.Node, fuse.Error) {
	dprintf("Lookup: %#v (ctx.key: %#v)", name, ctx.key)
	key := ctx.key + "/" + name
	if ctx.key == "" {
		key = name
	}
	kind, obj, ok := s3Lookup(key)
	if !ok {
		return nil, fuse.ENOENT
	}
	ctx.key = key
	ctx.kind = kind
	if obj != nil {
		ctx.size = uint64(*obj.ContentLength)
		ctx.body = obj.Body
	} else {
		ctx.size = 0
		ctx.body = nil
	}
	dprintf("Lookup: got %v", ctx)
	return ctx, nil
}

func (ctx s3fs) ReadDir(intr fuse.Intr) ([]fuse.Dirent, fuse.Error) {
	dprintf("ReadDir: %v", ctx)
	if ctx.kind != tPrefix {
		return nil, fuse.EIO
	}

	objs, tPrefixes, err := s3ListDir(ctx.key)
	if err != nil {
		dprintf("ReadDir: %v", err)
	}

	out := make([]fuse.Dirent, 0, len(objs)+len(tPrefixes))
	for _, obj := range objs {
		out = append(out, fuse.Dirent{Name: filepath.Base(obj)})
	}
	for _, pre := range tPrefixes {
		out = append(out, fuse.Dirent{Name: filepath.Base(pre)})
	}

	dprintf("ReadDir: got %d dirents: %v", len(out), out)
	return out, nil
}

func (ctx s3fs) ReadAll(intr fuse.Intr) ([]byte, fuse.Error) {
	dprintf("ReadAll: %v", ctx)
	defer ctx.body.Close()
	buf, err := ioutil.ReadAll(ctx.body)
	if err != nil {
		dprintf("Read: error reading %#v: %v", ctx.key, err)
		return nil, fuse.EIO
	}
	return buf, nil
}
