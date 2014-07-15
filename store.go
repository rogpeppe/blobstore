// The blobstore package implements a blob storage
// system layered on top of MongoDB's GridFS.
// Blobs with the same content share storage.
package blobstore

import (
	"crypto/sha256"
	"fmt"
	"hash/fnv"
	"io"
	"strings"
	"time"

	"github.com/zeebo/sbloom"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

// Storage represents a collection of named blobs held in a mongo
// database.
type Storage struct {
	fs *mgo.GridFS
}

// New returns a new storage value that stores its values in the
// given database. The collections created will be given names with the
// given prefix.
func New(db *mgo.Database, collectionPrefix string) *Storage {
	s := &Storage{
		fs: db.GridFS(collectionPrefix),
	}
	// TODO check error
	s.fs.Files.EnsureIndex(mgo.Index{
		Key: []string{"filename"},
	})
	return s
}

const blobRefPrefix = "blob-"

func hashName(h string) string {
	return blobRefPrefix + h
}

// Create creates a blob with the given name, reading
// the contents from the given reader. The sha256Hash
// parameter holds the sha256 hash of the blob's contents,
// encoded as ASCII hexadecimal.
//
// If a blob with the same content already exists in the store,
// that content will be reused and alreadyExists will be true.
// No data will have been read from r in this case.
func (s *Storage) Create(sha256Hash string, r io.Reader) (alreadyExists bool, err error) {
	blobRef := hashName(sha256Hash)
	changes, err := s.fs.Files.UpdateAll(
		bson.D{{"filename", blobRef}},
		bson.D{{"$set", bson.D{{"metadata.createtime", time.Now()}}}},
	)
	if err != nil {
		return false, err
	}
	if changes.Updated > 0 {
		return true, nil
	}
	f, err := s.fs.Create(blobRef)
	if err != nil {
		return false, err
	}
	if err := copyAndCheckHash(f, r, sha256Hash); err != nil {
		f.Abort()
		f.Close()
		return false, err
	}
	f.SetMeta(bson.D{{"createtime", time.Now()}})
	if err := f.Close(); err != nil {
		return false, err
	}
	return false, nil
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

// CollectGarbage reads hex SHA256 hashes from the
// given channel until it is closed, and then deletes any blobs that
// are not mentioned that have not been referenced
// since the given time.
func (s *Storage) CollectGarbage(refs <-chan string, before time.Time) error {
	filter := sbloom.NewFilter(fnv.New64(), 128)
	for ref := range refs {
		filter.Add([]byte(ref))
	}
	query := s.fs.Find(bson.D{{"metadata.createtime", bson.D{{"$lt", before}}}})
	query = query.Sort("filename")
	query = query.Batch(5000)
	previousBlob := ""
	var doc struct {
		Filename string
	}
	for iter := query.Iter(); iter.Next(&doc); {
		if !strings.HasPrefix(doc.Filename, blobRefPrefix) {
			// It's not a blob - ignore it.
			continue
		}
		if doc.Filename == previousBlob {
			if err := s.fs.Files.Remove(doc.Filename); err != nil && err != mgo.ErrNotFound {
				return fmt.Errorf("cannot remove duplicate blob %q: %v", doc.Filename, err)
			}
		}
		previousBlob = doc.Filename
		hash := doc.Filename[len(blobRefPrefix):]
		if filter.Lookup([]byte(hash)) {
			// The blob is (almost certainly) referenced.
			continue
		}
		// Prepare the file for deletion (but only if it has not been referenced
		// in the meantime.
		_, err := s.fs.Files.UpdateAll(
			bson.D{{"filename", doc.Filename}, {"metadata.createtime", bson.D{{"$lt", before}}}},
			bson.D{{"$set", bson.D{{"filename", "deleted"}}}},
		)
		if err != nil {
			return fmt.Errorf("cannot prepare file for deletion: %v", err)
		}
	}
	if err := s.fs.Remove("deleted"); err != nil && err != mgo.ErrNotFound {
		return fmt.Errorf("cannot delete files: %v", err)
	}
	return nil
}
