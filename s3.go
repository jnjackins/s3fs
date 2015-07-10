package main

import (
	"bytes"
	"io/ioutil"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	s3Client *s3.S3
	s3Bucket string
)

func s3Init(bucket string) {
	s3Client = s3.New(&aws.Config{Region: "ap-northeast-1"})
	s3Bucket = bucket
}

func s3Lookup(key string) (kind int, obj *s3.GetObjectOutput, ok bool) {
	if key == "" {
		dprintf("s3Lookup: %#v is a prefix", key)
		return tPrefix, nil, true
	}

	// first try an object lookup
	params1 := &s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(key),
	}
	resp, err := s3Client.GetObject(params1)
	if err == nil {
		dprintf("s3Lookup: %#v is an object", key)
		return tObject, resp, true
	}
	dprintf("s3Lookup: %#v is not an object", key)

	// otherwise, see if it is a valid prefix - i.e. list the parent "directory"
	// and check for an s3.CommonPrefix that matches key
	parent := filepath.Dir(key)
	if parent == "." {
		// workaround filepath.Dir behaviour
		parent = ""
	} else {
		parent += "/"
	}
	key += "/"
	dprintf("s3Lookup: looking for prefix match for %#v in %#v", key, parent)
	var marker string
	done := false
	for !done {
		params2 := &s3.ListObjectsInput{
			Bucket:    aws.String(s3Bucket),
			Marker:    aws.String(marker),
			Prefix:    aws.String(parent),
			Delimiter: aws.String("/"),
		}
		resp, err := s3Client.ListObjects(params2)
		if err != nil {
			dprintf("s3Lookup: error listing keys with prefix %#v: %v", parent, err)
		}
		if *resp.IsTruncated {
			marker = *resp.NextMarker
		} else {
			done = true
		}
		for _, p := range resp.CommonPrefixes {
			if *p.Prefix == key {
				dprintf("s3Lookup: %#v is a prefix", key)
				return tPrefix, nil, true
			}
			dprintf("s3Lookup: looking for prefix match: %#v != %#v", *p.Prefix, key)
		}
	}
	dprintf("s3Lookup: %#v is not a prefix", key)
	return 0, nil, false
}

// s3ListDir lists the objects with the given prefix, and subprefixes
// which do not contain extra delimiters.
func s3ListDir(prefix string) (objects []string, prefixes []string, err error) {
	objects = make([]string, 0)
	prefixes = make([]string, 0)

	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	dprintf("s3ListDir: looking for objects/subprefixes with prefix %#v", prefix)
	var marker string
	done := false
	for !done {
		params := s3.ListObjectsInput{
			Bucket:    aws.String(s3Bucket),
			Marker:    aws.String(marker),
			Prefix:    aws.String(prefix),
			Delimiter: aws.String("/"),
		}
		resp, err := s3Client.ListObjects(&params)
		if err != nil {
			return nil, nil, err
		}

		for _, o := range resp.Contents {
			if *o.Key != prefix {
				objects = append(objects, *o.Key)
			}
		}
		for _, p := range resp.CommonPrefixes {
			if *p.Prefix != prefix {
				prefixes = append(prefixes, *p.Prefix)
			}
		}

		if *resp.IsTruncated {
			marker = *resp.NextMarker
		} else {
			done = true
		}
	}

	dprintf("s3ListDir: objects in %#v: %v", prefix, objects)
	dprintf("s3ListDir: prefixes in %#v: %v", prefix, prefixes)
	return objects, prefixes, nil
}

func s3GetObj(key string) ([]byte, error) {
	params := s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(key),
	}
	resp, err := s3Client.GetObject(&params)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	return data, nil
}

func s3PutObj(key string, data []byte) error {
	params := s3.PutObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	_, err := s3Client.PutObject(&params)
	return err
}
