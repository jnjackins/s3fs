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
	return fmt.Sprintf("[%#v (%s)]", ctx.key, kind)
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
		dprintf("Attr: type: directory")
		return fuse.Attr{
			Mode: os.ModeDir | 0755,
			Uid:  serverUid,
			Gid:  serverGid,
		}
	case tObject:
		dprintf("Attr: type: regular file")
		return fuse.Attr{
			Mode: 0644,
			Size: ctx.size,
			Uid:  serverUid,
			Gid:  serverGid,
		}
	default:
		dprintf("Attr: type: unsupported")
		return fuse.Attr{}
	}
}

func (ctx s3fs) Lookup(name string, intr fuse.Intr) (fuse.Node, fuse.Error) {
	dprintf("Lookup: %#v (ctx.key: %#v)", name, ctx.key)
	var key string
	if ctx.key == "" {
		key = name
	} else {
		key = ctx.key + "/" + name
	}
	kind, obj, ok := s3Lookup(key)
	if !ok {
		dprintf("Lookup: returned ENOENT for %#v / %#v", ctx.key, name)
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
	if ctx.body == nil {
		dprintf("ReadAll: %v has nil body, doing lookup", ctx)
		root := ctx
		root.key = "" // we need to lookup ctx.key from a root ctx
		node, err := root.Lookup(ctx.key, intr)
		if err != nil {
			dprintf("ReadAll: %v: lookup failed: %v", ctx, err)
			return nil, fuse.EIO
		}
		ctx = node.(s3fs)
		if ctx.body == nil {
			dprintf("ReadAll: %v: body still nil after lookup", ctx)
			return nil, fuse.EIO
		}
		dprintf("ReadAll: lookup successful")
	}
	defer ctx.body.Close()
	buf, err := ioutil.ReadAll(ctx.body)
	if err != nil {
		dprintf("ReadAll: error reading %#v: %v", ctx.key, err)
		return nil, fuse.EIO
	}
	dprintf("ReadAll: read %d bytes", len(buf))
	return buf, nil
}

func (ctx s3fs) Create(req *fuse.CreateRequest, res *fuse.CreateResponse, intr fuse.Intr) (fuse.Node, fuse.Handle, fuse.Error) {
	dprintf("Create: %v (ctx.key = %#v)", req, ctx.key)
	if ctx.key == "" {
		ctx.key = req.Name
	} else {
		ctx.key += "/" + req.Name
	}
	if req.Mode&os.ModeDir > 0 {
		ctx.kind = tPrefix
	} else {
		ctx.kind = tObject
		err := s3PutObj(ctx.key, []byte{})
		if err != nil {
			dprintf("Create: error writing to %#v: %v", ctx.key, err)
			return nil, nil, fuse.EIO
		}
	}
	dprintf("Create: done creating %#v", req.Name)
	return ctx, ctx, nil
}

func (ctx s3fs) Remove(req *fuse.RemoveRequest, intr fuse.Intr) fuse.Error {
	dprintf("Remove: %v (ctx.key = %#v)", req, ctx.key)
	var key string
	if ctx.key == "" {
		key = req.Name
	} else {
		key = ctx.key + "/" + req.Name
	}
	err := s3RemoveObj(key)
	if err != nil {
		dprintf("Remove: error removing %#v: %v", key, err)
		return fuse.EIO
	}
	dprintf("Remove: successfully removed %#v", key)
	return nil
}

func (ctx s3fs) WriteAll(buf []byte, intr fuse.Intr) fuse.Error {
	dprintf("WriteAll: %v", ctx)
	err := s3PutObj(ctx.key, buf)
	if err != nil {
		dprintf("WriteAll: error writing to %#v: %v", ctx.key, err)
		return fuse.EIO
	}
	dprintf("WriteAll: done writing to %#v", ctx.key)
	return nil
}
