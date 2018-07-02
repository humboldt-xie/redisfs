// Hellofs implements a simple "hello world" file system.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"github.com/go-redis/redis"
	"golang.org/x/net/context"
	"strconv"
)

var block_size = uint64(5 * 1024)
var zero_block []byte

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func UInt64(v64 string) uint64 {
	if s, err := strconv.ParseInt(v64, 10, 64); err == nil {
		//fmt.Printf("%T, %v\n", s, s)
		return uint64(s)
	}
	return uint64(0)
}
func init() {
	zero_block = make([]byte, block_size)
}

var client *redis.Client

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}

	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	//fmt.Printf("all client\n")
	mountpoint := flag.Arg(0)
	//fuse.Unmount(mountpoint)

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("helloredis"),
		fuse.Subtype("redisfs"),
		fuse.LocalVolume(),
		fuse.VolumeName("Hello world!"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = fs.Serve(c, &FS{})
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

// FS implements the hello world file system.
type FS struct{}

func (*FS) Root() (fs.Node, error) {
	//fmt.Printf("root fs")
	return &Dir{Inode: 0}, nil
}

func (*FS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {

	resp.Blocks = 999999            //uint64 // Total data blocks in file system.
	resp.Bfree = 99999              //uint64 // Free blocks in file system.
	resp.Bavail = 9999              //uint64 // Free blocks in file system if you're not root.
	resp.Files = 9999               //uint64 // Total files in file system.
	resp.Ffree = 9999               //uint64 // Free files in file system.
	resp.Bsize = uint32(block_size) //uint32 // Block size
	resp.Namelen = 256              //uint32 // Maximum file name length?
	//resp.Frsize             //uint32 // Fragment size, smallest addressable data size in the file system.
	return nil
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	Inode uint64
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = d.Inode
	a.Mode = os.ModeDir | 0666
	return nil
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	fmt.Printf("mkdir %s\n", req.String())
	inode, err := client.Incr("inode").Result()
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("d-%d", d.Inode)
	_, err = client.HSet(key, req.Name, inode).Result()
	if err != nil {
		fmt.Printf("lookup err %s\n", err)
		return nil, err
	}
	dir := &Dir{
		Inode: uint64(inode),
	}
	var a fuse.Attr
	a.Mode = req.Mode
	//a.Size = 0
	//a.Blocks = 0
	//a.BlockSize = uint32(block_size)
	a.Inode = uint64(inode)
	a.Atime = time.Now()
	a.Ctime = time.Now()
	a.Mtime = time.Now()
	a.Crtime = time.Now()
	err = write_attr(&a)
	if err != nil {
		return nil, err
	}
	err = client.HSet(key, req.Name, fmt.Sprintf("%d", inode)).Err()
	if err != nil {
		return nil, err
	}
	return dir, nil

}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	fmt.Printf("create %s %s\n", req.String(), resp.String())

	inode, err := client.Incr("inode").Result()
	if err != nil {
		return nil, nil, err
	}
	file := &File{
		Inode: uint64(inode),
	}
	var a fuse.Attr
	a.Mode = req.Mode
	a.Size = 0
	a.Blocks = 0
	a.BlockSize = uint32(block_size)
	a.Inode = uint64(inode)
	a.Atime = time.Now()
	a.Ctime = time.Now()
	a.Mtime = time.Now()
	a.Crtime = time.Now()
	err = write_attr(&a)
	if err != nil {
		return nil, nil, err
	}
	key := fmt.Sprintf("d-%d", d.Inode)
	err = client.HSet(key, req.Name, fmt.Sprintf("%d", inode)).Err()
	if err != nil {
		return nil, nil, err
	}
	return file, file, nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	key := fmt.Sprintf("d-%d", d.Inode)
	v, err := client.HGet(key, name).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fuse.ENOENT
		}
		fmt.Printf("lookup err %s\n", err)
		return nil, err
	}
	in, _ := strconv.Atoi(v)
	inode := uint64(in)
	fu := fuse.Attr{}
	err = GetAttr(inode, &fu)
	if err != nil {
		return nil, err
	}
	if (fu.Mode & os.ModeDir) != 0 {
		return &Dir{
			Inode: inode,
		}, nil
	}

	return &File{
		Inode: inode,
	}, nil
	//return nil, fuse.ENOENT
}

func GetAttr(inode uint64, a *fuse.Attr) error {
	key := fmt.Sprintf("a-%d", inode)
	attr, err := client.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			return fuse.ENOENT
		}
		return err
	}
	err = json.Unmarshal([]byte(attr), a)
	//fmt.Printf("%#v \n", a)
	if err != nil {
		return err
	}
	return err

}

var dirDirs = []fuse.Dirent{
	{Inode: 2, Name: "hello", Type: fuse.DT_Dir},
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	//return dirDirs, nil
	//fmt.Printf("start all dir %s\n", d.Name)
	key := fmt.Sprintf("d-%d", d.Inode)
	dirs, err := client.HGetAll(key).Result()
	//fmt.Printf("all dir: %v %s\n", dirs, err)
	if err != nil {
		return nil, err
	}
	rdirs := []fuse.Dirent{}
	for k, v := range dirs {
		in, _ := strconv.Atoi(v)
		inode := uint64(in)
		name := k
		fu := fuse.Attr{}
		err := GetAttr(inode, &fu)
		if err != nil {
			continue
		}
		var df fuse.Dirent
		//fmt.Printf("%s %s\n", t, name)
		if (fu.Mode | os.ModeDir) != 0 {
			df = fuse.Dirent{Name: name, Inode: inode, Type: fuse.DT_Dir}
		} else {
			df = fuse.Dirent{Name: name, Inode: inode, Type: fuse.DT_File}
		}
		rdirs = append(rdirs, df)
	}
	return rdirs, nil
}

// File implements both Node and Handle for the hello file.
type File struct {
	Inode uint64
}

/*func (f *File) meta() (uint64, error) {
	key := fmt.Sprintf("meta-%d", f.Inode)
	m, err := client.Get(key).Result()
	if err != nil {
		return 0, err
	}
	return UInt64(m), nil
	var fu fuse.Attr
	err = GetAttr(inode, &fu)
	if err != nil {
		return 0, err
	}
	return fu.Blocks, err
}*/

func (f *File) write_block(block uint64, data []byte, offset int64) error {
	key := fmt.Sprintf("b-%d-%d", f.Inode, block)
	//fmt.Printf("write block %s %d %s\n", key, len(data), string(data))
	err := client.SetRange(key, offset, string(data)).Err()
	if err != nil {
		return err
	}
	return nil
}

func (f *File) read_block(block uint64, offset, size int64) ([]byte, error) {
	key := fmt.Sprintf("b-%d-%d", f.Inode, block)
	m, err := client.GetRange(key, offset, size).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		return []byte{}, err
	}
	b := []byte(m)
	return b, nil
}

func (f *File) app(data []byte, block []byte, size int) []byte {
	if size > len(block) {
		block = append(block, zero_block[0:size-len(block)]...)
	}
	return append(data, block[0:size]...)
}

func (f *File) write(offset uint64, data []byte) (size int, err error) {
	var fu fuse.Attr
	err = GetAttr(f.Inode, &fu)
	if err != nil {
		return 0, err
	}
	/*m, err := f.meta()
	if err != nil {
		return err
	}*/
	size = len(data)
	toffset := offset
	for len(data) > 0 {
		block := toffset / block_size
		block_offset := toffset % block_size
		block_end := block_offset + uint64(len(data))
		if block_end > block_size {
			block_end = block_size
		}
		data_size := block_end - block_offset
		err = f.write_block(block, data[:data_size], int64(block_offset))
		if err != nil {
			return 0, err
		}
		toffset += data_size
		data = data[data_size:]
	}
	if offset+uint64(size) > fu.Size {
		fu.Size = offset + uint64(size)
		err = write_attr(&fu)
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

func (f *File) read(offset uint64, size uint64) (data []byte, err error) {
	var fu fuse.Attr
	err = GetAttr(f.Inode, &fu)
	if err != nil {
		return []byte{}, err
	}
	if offset >= fu.Size {
		return []byte{}, nil
	}
	if offset+size > fu.Size {
		size = fu.Size - offset
	}

	start_block := offset / block_size
	end_block := (offset + size) / block_size
	block_offset := offset - start_block*block_size
	block_end := block_size
	remain_size := size
	for i := start_block; i <= end_block; i++ {
		block, err := f.read_block(i, int64(block_offset), int64(block_end))
		if err != nil {
			return []byte{}, err
		}
		if remain_size < block_end-block_offset {
			block_end = block_offset + remain_size
		}
		data = f.app(data, block, int(block_end-block_offset))
		fmt.Printf("block %d %d\n", block_offset, block_end)
		remain_size -= block_end - block_offset
		block_offset = 0
	}
	//fmt.Printf("read %d %d %d %d\n", offset, size, len(data), fu.Size)

	return data, nil
}

func write_attr(a *fuse.Attr) error {
	b, err := json.Marshal(a)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("a-%d", a.Inode)
	err = client.Set(key, b, 0).Err()
	if err != nil {
		return err
	}
	return nil
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	GetAttr(f.Inode, a)
	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	//if f.conf.directIO {
	//	resp.Flags |= fuse.OpenDirectIO
	//}
	resp.Flags |= fuse.OpenDirectIO
	fmt.Printf("open %s %s\n", req.String(), resp.String())
	//resp.Flags |= fuse.OpenKeepCache
	return f, nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	//fmt.Printf("write %d %d %d\n", f.Inode, req.Offset, len(req.Data))
	size, err := f.write(uint64(req.Offset), req.Data)
	if err != nil {
		return err
	}
	resp.Size = size
	return nil

}
func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	d, err := f.read(uint64(req.Offset), uint64(req.Size))
	if err != nil {
		return err
	}
	resp.Data = d
	return nil

}
