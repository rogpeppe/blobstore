// The blobstore package implements a named-blob storage
// system layered on top of MongoDB's GridFS.
//
// Files with the same content share storage.
package blobstore

import (
	"crypto/rand"
	"crypto/sha256"
	//	"hash/fnv"
	"fmt"
	"io"
	//	"log"

	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	//	"github.com/zeebo/sbloom"
)

// Storage represents a collection of named blobs held in a mongo
// database.
type Storage struct {
	fs           *mgo.GridFS
	catalog      *mgo.Collection
	gcGeneration *mgo.Collection
}

// New returns a new storage value that stores its values in the
// given database. The collections created will be given names with the
// given prefix.
func New(db *mgo.Database, collectionPrefix string) *Storage {
	return &Storage{
		fs:           db.GridFS(collectionPrefix + ".grid"),
		catalog:      db.C(collectionPrefix + ".catalog"),
		gcGeneration: db.C(collectionPrefix + ".gcgen"),
	}
}

type catalogDoc struct {
	Name    string `bson:"_id"`
	BlobRef string `bson:"blobref"`
}

func hashName(h string) string {
	return "blob-" + h
}

func randomName() (string, error) {
	var data []byte
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", data), nil
}

var generationInc = mgo.Change{
	Update: bson.D{{"$inc", bson.D{{"generation", 1}}}},
	Upsert: true,
}

func (s *Storage) incrementGCGeneration() (int64, error) {
	var result struct {
		Generation int64
	}
	_, err := s.gcGeneration.Find(nil).Apply(generationInc, &result)
	if err != nil {
		return 0, err
	}
	return result.Generation, nil
}

// Create creates a file with the given name. The sha256Hash
// parameter holds the sha256 hash of the file's contents,
// encoded as ASCII hexadecimal.
//
// If a file with the same content does not exist in the store,
// it will be read from the given reader.
func (s *Storage) Create(name string, sha256Hash string, r io.Reader) (err error) {
	//	_, err := s.incrementGCGeneration()
	//	if err != nil {
	//		return err
	//	}

	blobRef := hashName(sha256Hash)
	f, err := s.fs.Open(blobRef)
	if err == nil {
		//		find doc with ref > 0, set ref to 1
		// The blob already exists
		f.Close()
		return nil
	}
	f, err = s.fs.Create(blobRef)
	if err != nil {
		return err
	}
	defer func() {
		if f != nil {
			f.Close()
		}
		if err != nil {
			// TODO use Abort method when it's implemented
			//			s.fs.Remove(fileId)
		}
	}()
	if err := copyAndCheckHash(f, r, sha256Hash); err != nil {
		return err
	}
	f.SetName(blobRef)

	if err := f.Close(); err != nil {
		f = nil
		if !isDuplicateIndexError(err) {
			return err
		}
		// someone else has created the blob at the same time as us
		// so remove the duplicate data but carry on to create the file.
		//		s.fs.Remove(fileId)
	}
	f = nil

	_, err = s.catalog.UpsertId(name, bson.D{{"$set", bson.D{{"blobref", blobRef}}}})
	if err != nil {
		return fmt.Errorf("cannot update catalog: %v", err)
	}
	return nil
}

func isDuplicateIndexError(err error) bool {
	// TODO
	return false
}

func copyAndCheckHash(w io.Writer, r io.Reader, sha256Hash string) error {
	sha256hash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(w, sha256hash), r); err != nil {
		return err
	}
	actualHash := fmt.Sprintf("%x", sha256hash.Sum(nil))
	if actualHash != sha256Hash {
		return fmt.Errorf("file checksum mismatch")
	}
	return nil
}

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// Open opens the file with the given name.
func (s *Storage) Open(name string) (ReadSeekCloser, error) {
	var doc catalogDoc

	err := s.catalog.Find(bson.D{{"_id", name}}).One(&doc)
	if err != nil {
		return nil, fmt.Errorf("cannot find name: %v", err)
	}
	f, err := s.fs.Open(doc.BlobRef)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Remove removes the file with the given name.
func (s *Storage) Remove(name string) error {
	return s.catalog.Remove(bson.D{{"_id", name}})
}

// CollectGarbage scans through all files and blobs,
// and deletes blobs that are unreferenced by any
// file.
func (s *Storage) CollectGarbage() error {
	//	filter := sbloom.Filter(fnv.New64(), 128)
	//
	//	err := s.catalog.Find(nil).Batch(5000)
	//
	//	find all docs with no references
	return nil
}
