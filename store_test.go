package blobstore_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	stdtesting "testing"

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
	hashBytes := sha256.Sum256(data)
	hash := fmt.Sprintf("%x", hashBytes)
	err := store.Create("some-name", hash, bytes.NewReader(data))
	c.Assert(err, gc.IsNil)

	f, err := store.Open("some-name")
	c.Assert(err, gc.IsNil)
	defer f.Close()
	gotData, err := ioutil.ReadAll(f)
	c.Assert(err, gc.IsNil)
	c.Assert(gotData, gc.DeepEquals, data)
}

func (s *storeSuite) TestIncrementGCGeneration(c *gc.C) {
	db := s.Session.DB("a-database")
	store := blobstore.New(db, "prefix")

	for i := 0; i < 10; i++ {
		gen, err := store.IncrementGCGeneration()
		c.Assert(err, gc.IsNil)
		c.Assert(gen, gc.Equals, int64(i))
	}
}

func (s *storeSuite) BenchmarkIncrementGCGeneration(c *gc.C) {
	for i := c.N - 1; i >= 0; i-- {
		db := s.Session.DB("a-database")
		store := blobstore.New(db, "prefix")
		_, err := store.IncrementGCGeneration()
		c.Assert(err, gc.IsNil)
	}
}
