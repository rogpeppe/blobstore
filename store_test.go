package blobstore_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	stdtesting "testing"

	"labix.org/v2/mgo"
	"github.com/juju/testing"
	"github.com/rogpeppe/blobstore"
	gc "launchpad.net/gocheck"
)

func TestPackage(t *stdtesting.T) {
	testing.MgoTestPackage(t, nil)
}

type storeSuite struct {
	testing.MgoSuite
}

var _ = gc.Suite(&storeSuite{})

func (s *storeSuite) TestCreateOpen(c *gc.C) {
	db := s.Session.DB("a-database")
	store := blobstore.New(db, "prefix")

	data := []byte(`some file data`)
	hash := hashOf(data)
	err := store.Create(hash, bytes.NewReader(data))
	c.Assert(err, gc.IsNil)

	f, err := store.Open(hash)
	c.Assert(err, gc.IsNil)
	defer f.Close()
	gotData, err := ioutil.ReadAll(f)
	c.Assert(err, gc.IsNil)
	c.Assert(gotData, gc.DeepEquals, data)
}

func (s *storeSuite) TestInvalidHash(c *gc.C) {
	db := s.Session.DB("a-database")
	store := blobstore.New(db, "prefix")

	data := []byte(`some file data`)
	hash := hashOf([]byte("foo"))

	err := store.Create(hash, bytes.NewReader(data))
	c.Assert(err, gc.ErrorMatches, "file checksum mismatch")

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
	db := s.Session.DB("a-database")
	store := blobstore.New(db, "prefix")

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
		err := store.Create(hash, src1)
		done1 <- err
	}()
	done2 := make(chan error)
	go func() {
		err := store.Create(hash, src2)
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

	// Check that we can read the blob.
	f, err := store.Open(hash)
	c.Assert(err, gc.IsNil)

	hasher.Reset()
	io.Copy(hasher, f)
	c.Assert(hex(hasher.Sum(nil)), gc.Equals, hash)
}

type dataSource struct {
	buf []byte
	bufIndex int
	remain int64
}

func newDataSource(fillWith int64, size int64) io.Reader {
	src := &dataSource{
		remain: size,
	}
	for len(src.buf) < 8 * 1024 {
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
	r io.Reader
	resume chan chan struct{}
}

func newDelayedEOFReader(r io.Reader, resume chan chan struct{}) io.Reader {
	return &delayedEOFReader{
		r: r,
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
