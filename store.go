// The blobstore package implements a blob storage
// system layered on top of MongoDB's GridFS.
// Blobs with the same content share storage.
package blobstore

import (
	"crypto/sha256"
//	"hash/fnv"
	"fmt"
	"io"
	"log"
	"time"

	"labix.org/v2/mgo"
//	"github.com/zeebo/sbloom"
)

// Storage represents a collection of named blobs held in a mongo
// database.
type Storage struct {
	fs           *mgo.GridFS
}

// New returns a new storage value that stores its values in the
// given database. The collections created will be given names with the
// given prefix.
func New(db *mgo.Database, collectionPrefix string) *Storage {
	return &Storage{
		fs:           db.GridFS(collectionPrefix + ".grid"),
	}
}

func hashName(h string) string {
	return "blob-" + h
}

// Create creates a file with the given name. The sha256Hash
// parameter holds the sha256 hash of the file's contents,
// encoded as ASCII hexadecimal.
//
// If a file with the same content does not exist in the store,
// it will be read from the given reader.
func (s *Storage) Create(sha256Hash string, r io.Reader) (err error) {
	blobRef := hashName(sha256Hash)
	f, err := s.fs.Open(blobRef)
	if err == nil {
		// The blob already exists

		// TODO change last reference time to current time.
		f.Close()
		return checkHash(r, sha256Hash)
	}
	f, err = s.fs.Create(blobRef)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			// TODO use Abort method when it's implemented
			f.SetName("aborted")
		}
		closeErr := f.Close()
		if closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				log.Printf("failed to close file after error: %v", closeErr)
			}
		}
	}()
	if err := copyAndCheckHash(f, r, sha256Hash); err != nil {
		return err
	}
	f.SetName(blobRef)
	return nil
}

func checkHash(r io.Reader, sha256Hash string) error {
	hash := sha256.New()
	if _, err := io.Copy(hash, r); err != nil {
		return err
	}
	gotSum := fmt.Sprintf("%x", hash.Sum(nil))
	if gotSum != sha256Hash {
		return fmt.Errorf("hash mismatch - you don't have the right data!")
	}
	return nil
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
func (s *Storage) Open(sha256hash string) (ReadSeekCloser, error) {
	f, err := s.fs.Open(hashName(sha256hash))
	if err != nil {
		return nil, err
	}
	return f, nil
}

// CollectGarbage reads blob references from the
// given channel until it is closed, and then deletes any blobs that
// are not mentioned that have not been referenced
// since the given time.
func (s *Storage) CollectGarbage(refs <-chan string, before time.Time) error {
//	filter := sbloom.Filter(fnv.New64(), 128)
//	for ref := range refs {
//		filter.Add([]byte(ref))
//	}
//	err := s.fs.Find(nil).Batch(5000).Filter(time < before)
//	iterate {
//		if name == "aborted" {
//			delete
//		}
//		if item not found {
//			delete item if time < before
//		}
//	}
	return nil
}
