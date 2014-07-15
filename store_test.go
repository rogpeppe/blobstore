package blobstore_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"sync"
	stdtesting "testing"

	"github.com/juju/testing"
	"github.com/rogpeppe/blobstore"
	"labix.org/v2/mgo"
	gc "launchpad.net/gocheck"
)

func TestPackage(t *stdtesting.T) {
	testing.MgoTestPackage(t, nil)
}

type storeSuite struct {
	testing.MgoSuite
}

var _ = gc.Suite(&storeSuite{})

func (s *storeSuite) TestCreateOpenCheck(c *gc.C) {
	store := blobstore.New(s.Session.DB("a-database"), "prefix")

	data := []byte(`some file data`)
	hash := hashOf(data)
	exists, err := store.Create(hash, bytes.NewReader(data))
	c.Assert(err, gc.IsNil)
	c.Assert(exists, gc.Equals, false)

	f, err := store.Open(hash)
	c.Assert(err, gc.IsNil)
	defer f.Close()
	gotData, err := ioutil.ReadAll(f)
	c.Assert(err, gc.IsNil)
	c.Assert(gotData, gc.DeepEquals, data)

	ok, size, err := store.Check(hash)
	c.Assert(err, gc.IsNil)
	c.Assert(ok, gc.Equals, true)
	c.Assert(size, gc.Equals, int64(len(data)))

	ok, size, err = store.Check(hashOf([]byte("foo")))
	c.Assert(err, gc.IsNil)
	c.Assert(ok, gc.Equals, false)
	c.Assert(size, gc.Equals, int64(0))
}

func (s *storeSuite) TestInvalidHash(c *gc.C) {
	store := blobstore.New(s.Session.DB("a-database"), "prefix")

	data := []byte(`some file data`)
	hash := hashOf([]byte("foo"))

	exists, err := store.Create(hash, bytes.NewReader(data))
	c.Assert(err, gc.ErrorMatches, "file checksum mismatch")
	c.Assert(exists, gc.Equals, false)

	f, err := store.Open(hash)
	c.Assert(err, gc.Equals, mgo.ErrNotFound)
	c.Assert(f, gc.IsNil)
}

func hashOf(data []byte) string {
	hashBytes := sha256.Sum256(data)
	return hex(hashBytes[:])
}

func hex(data []byte) string {
	return fmt.Sprintf("%x", data)
}

func (s *storeSuite) TestSimultaneousUploads(c *gc.C) {
	store := blobstore.New(s.Session.DB("a-database"), "prefix")

	const size = 10 * 1024 * 1024
	hasher := sha256.New()
	io.Copy(hasher, newDataSource(0, size))
	hash := hex(hasher.Sum(nil))

	src1 := newDataSource(0, size)
	src2 := newDataSource(0, size)

	resume := make(chan chan struct{})
	src1 = newDelayedEOFReader(src1, resume)
	src2 = newDelayedEOFReader(src2, resume)

	done1 := make(chan error)
	go func() {
		exists, err := store.Create(hash, src1)
		c.Check(exists, gc.Equals, false)
		done1 <- err
	}()
	done2 := make(chan error)
	go func() {
		exists, err := store.Create(hash, src2)
		c.Check(exists, gc.Equals, false)
		done2 <- err
	}()

	// Wait for all data to be read from both.
	reply1 := <-resume
	reply2 := <-resume

	// Race to finish.
	close(reply1)
	close(reply2)

	// Wait for the Creates to succeed
	err1 := <-done1
	err2 := <-done2

	c.Assert(err1, gc.IsNil)
	c.Assert(err2, gc.IsNil)

	assertBlob(c, store, hash)
}

func (s *storeSuite) TestCreateRemove(c *gc.C) {
	store := blobstore.New(s.Session.DB("a-database"), "prefix")

	data := []byte(`some file data`)
	hash := hashOf(data)
	exists, err := store.Create(hash, bytes.NewReader(data))
	c.Assert(err, gc.IsNil)
	c.Assert(exists, gc.Equals, false)

	r := bytes.NewReader(data)
	exists, err = store.Create(hash, bytes.NewReader(data))
	c.Assert(err, gc.IsNil)
	c.Assert(exists, gc.Equals, true)

	// Check that no bytes have been read.
	n, _ := r.Read(make([]byte, len(data)))
	c.Assert(n, gc.Equals, len(data))

	err = store.Remove(hash)
	c.Assert(err, gc.IsNil)

	// The blob should still exist because there's still a reference to it.
	assertBlob(c, store, hash)

	err = store.Remove(hash)
	c.Assert(err, gc.IsNil)

	// The last reference has been removed, and the blob with it.
	f, err := store.Open(hash)
	c.Assert(err, gc.Equals, mgo.ErrNotFound)
	c.Assert(f, gc.IsNil)
}

func (s *storeSuite) TestConcurrentCreateRemove(c *gc.C) {
	data := []byte(`some file data`)
	hash := hashOf(data)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session := s.Session.Clone()
			defer session.Close()
			store := blobstore.New(session.DB("a-database"), "prefix")
			for i := 0; i < 100; i++ {
				exists, err := store.Create(hash, bytes.NewReader(data))
				c.Check(err, gc.IsNil)
				c.Logf("exists %v", exists)
				err = store.Remove(hash)
				c.Check(err, gc.IsNil)
			}
		}()
	}
	wg.Wait()

	store := blobstore.New(s.Session.DB("a-database"), "prefix")
	// All references should have been removed, and the blob with it.
	f, err := store.Open(hash)
	c.Assert(err, gc.Equals, mgo.ErrNotFound)
	c.Assert(f, gc.IsNil)
}

// assertBlob checks that the store holds the blob with
// the given hash.
func assertBlob(c *gc.C, store *blobstore.Storage, hash string) {
	f, err := store.Open(hash)
	c.Assert(err, gc.IsNil)
	defer f.Close()

	hasher := sha256.New()
	io.Copy(hasher, f)
	c.Assert(hex(hasher.Sum(nil)), gc.Equals, hash)
}

type dataSource struct {
	buf      []byte
	bufIndex int
	remain   int64
}

func newDataSource(fillWith int64, size int64) io.Reader {
	src := &dataSource{
		remain: size,
	}
	for len(src.buf) < 8*1024 {
		src.buf = strconv.AppendInt(src.buf, fillWith, 10)
		src.buf = append(src.buf, ' ')
	}
	return src
}

func (s *dataSource) Read(buf []byte) (int, error) {
	if int64(len(buf)) > s.remain {
		buf = buf[:int(s.remain)]
	}
	total := len(buf)
	if total == 0 {
		return 0, io.EOF
	}

	for len(buf) > 0 {
		if s.bufIndex == len(s.buf) {
			s.bufIndex = 0
		}
		nb := copy(buf, s.buf[s.bufIndex:])
		s.bufIndex += nb
		buf = buf[nb:]
		s.remain -= int64(nb)
	}
	return total, nil
}

type delayedEOFReader struct {
	r      io.Reader
	resume chan chan struct{}
}

func newDelayedEOFReader(r io.Reader, resume chan chan struct{}) io.Reader {
	return &delayedEOFReader{
		r:      r,
		resume: resume,
	}
}

func (r *delayedEOFReader) Read(buf []byte) (int, error) {
	n, err := r.r.Read(buf)
	if err == nil {
		return n, nil
	}
	reply := make(chan struct{})
	r.resume <- reply
	<-reply
	return n, err
}
