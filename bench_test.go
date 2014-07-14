package blobstore_test

import (
	"crypto/sha256"
	"fmt"
	"github.com/juju/juju/state/storage"
	jujuTxn "github.com/juju/juju/state/txn"
	"github.com/juju/testing"
	"github.com/rogpeppe/blobstore"
	"io"
	"labix.org/v2/mgo/txn"
	gc "launchpad.net/gocheck"
)

type benchmarkSuite struct {
	testing.MgoSuite
}

var _ = gc.Suite(&benchmarkSuite{})

func (s *benchmarkSuite) BenchmarkCreate(c *gc.C) {
	db := s.Session.DB("a-database")
	store := blobstore.New(db, "prefix")

	const fileSize = 30 * 1024
	hasher := sha256.New()
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		hasher.Reset()
		src := newDataSource(int64(i), fileSize)
		io.Copy(hasher, src)
		hash := hex(hasher.Sum(nil))
		exists, err := store.Create(hash, newDataSource(int64(i), fileSize))
		c.Assert(err, gc.IsNil)
		c.Assert(exists, gc.Equals, false)
	}
}

func (s *benchmarkSuite) BenchmarkManagedStorageCreate(c *gc.C) {
	db := s.Session.DB("a-database")
	txnRunner := jujuTxn.NewRunner(txn.NewRunner(db.C("txns")))
	rstore := storage.NewGridFS(db.Name, "prefix", s.Session)
	store := storage.NewManagedStorage(db, txnRunner, rstore)
	c.ResetTimer()
	const fileSize = 30 * 1024
	for i := 0; i < c.N; i++ {
		src := newDataSource(int64(i), fileSize)
		err := store.PutForEnvironment("env-uuid", fmt.Sprintf("file%d", i), src, fileSize)
		c.Assert(err, gc.IsNil)
	}
}
