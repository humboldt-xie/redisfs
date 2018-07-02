package main

import (
	"bazil.org/fuse"
	"github.com/go-redis/redis"
	"testing"
	"time"
)

func init() {
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}
func TestWrite(t *testing.T) {
	f := &File{Inode: 0}
	_, err := f.write(5, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.write(block_size, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.write(block_size+8, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestApp(t *testing.T) {
	f := &File{Inode: 0}
	data := f.app([]byte("h"), []byte("aello"), 1, 50)
	if len(data) != 51 {
		t.Fatal(data)
	}
}

func TestFile(t *testing.T) {
	f := &File{Inode: 1}
	var a fuse.Attr
	a.Mode = 0666
	a.Size = 0
	a.Blocks = 0
	a.BlockSize = uint32(block_size)
	a.Inode = f.Inode
	a.Atime = time.Now()
	a.Ctime = time.Now()
	a.Mtime = time.Now()
	a.Crtime = time.Now()
	f.write_attr(&a)
	f.write(0, []byte("hello"))
}
