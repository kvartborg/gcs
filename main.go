package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/juju/ratelimit"
)

var (
	upload  int
	timeout int
)

func init() {
	flag.IntVar(&upload, "u", 125000, "Upload speed in kb/s")
	flag.IntVar(&timeout, "t", 600, "timeout in seconds")
}

func main() {
	flag.Parse()

	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "first argument should be a local file and the second argument should be a google cloud bucket path")
		os.Exit(1)
	}

	filepath := os.Args[len(os.Args)-2]
	gcsparts := strings.Split(strings.Replace(os.Args[len(os.Args)-1], "gs://", "", 1), "/")
	gcsbucket := gcsparts[0]
	gcspath := strings.TrimRight(strings.Join(gcsparts[1:], "/"), "/")

	start := time.Now()
	fmt.Println(start)
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	defer client.Close()
	if err != nil {
		log.Fatal(err)
	}

	tctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	f, err := os.Open(filepath)

	if err != nil {
		log.Fatal(err)
	}

	b, err := ioutil.ReadAll(f)

	if err != nil {
		log.Fatal(err)
	}

	w := client.Bucket(gcsbucket).Object(gcspath + "/" + filepath).NewWriter(tctx)
	w.ContentType = http.DetectContentType(b)
	w.CRC32C = crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli))
	w.SendCRC32C = true

	bucket := ratelimit.NewBucketWithRate(float64(upload*1024), int64(upload*1024))
	_, err = io.Copy(w, ratelimit.Reader(bytes.NewReader(b), bucket))

	if err != nil {
		log.Fatal("write: ", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("close: ", err)
	}

	os.Remove(filepath)

	fmt.Println(time.Since(start))
}
