// The blobstore package implements a blob storage
// system layered on top of MongoDB's GridFS.
// Blobs with the same content share storage.
package blobstore

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log"

	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

// Storage represents a collection of named blobs held in a mongo
// database.
type Storage struct {
	fs *mgo.GridFS
}

// New returns a new Storage that stores blobs in the
// given database. The collections created will be given names with the
// given prefix.
func New(db *mgo.Database, collectionPrefix string) *Storage {
	s := &Storage{
		fs: db.GridFS(collectionPrefix),
	}
	// TODO check error
	s.fs.Files.EnsureIndex(mgo.Index{
		Key:    []string{"filename"},
		Unique: true,
	})
	return s
}

func hashName(h string) string {
	return "blob-" + h
}

type refCountMeta struct {
	RefCount int
}

// Check reports whether a blob with the given hash currently
// exists in the storage.
func (s *Storage) Check(sha256Hash string) (exists bool, size int64, err error) {
	f, err := s.fs.Open(hashName(sha256Hash))
	if err != nil && err != mgo.ErrNotFound {
		return false, 0, err
	}
	if err == nil {
		size := f.Size()
		f.Close()
		return true, size, nil
	}
	return false, 0, nil
}

// Create creates a blob with the given name, reading
// the contents from the given reader. The sha256Hash
// parameter holds the sha256 hash of the blob's contents,
// encoded as ASCII hexadecimal.
//
// If a blob with the same content already exists in the store,
// that content will be reused, its reference count
// incremented, and alreadyExists will be true.
// No data will have been read from r in this case.
func (s *Storage) Create(sha256Hash string, r io.Reader) (alreadyExists bool, err error) {
	blobRef := hashName(sha256Hash)

	// First try to increment the file's reference count.
	err = s.incRefCount(blobRef)
	if err == nil {
		return true, nil
	}
	if err != mgo.ErrNotFound {
		return false, err
	}
	f, err := s.fs.Create(blobRef)
	if err != nil {
		return false, err
	}
	f.SetMeta(refCountMeta{RefCount: 1})
	f.SetName(blobRef)
	if err := copyAndCheckHash(f, r, sha256Hash); err != nil {
		// Remove any chunks that were written while we were checking the hash.
		f.Abort()
		if closeErr := f.Close(); closeErr != nil {
			// TODO add mgo.ErrAborted so that we can avoid a string error check.
			if closeErr.Error() != "write aborted" {
				log.Printf("cannot clean up after hash-mismatch file write: %v", closeErr)
			}
		}
		return false, err
	}

	err = f.Close()
	if err == nil {
		return false, nil
	}
	if !mgo.IsDup(err) {
		return false, err
	}
	// We cannot close the file because of a clashing index,
	// which means someone else has created the blob first,
	// so all we need to do is increment the ref count.
	err = s.incRefCount(blobRef)
	if err == nil {
		// Although technically, the content already exists,
		// we have already read the content from the reader,
		// so report alreadyExists=false.
		return false, nil
	}
	if err != mgo.ErrNotFound {
		return false, fmt.Errorf("cannot increment blob ref count: %v", err)
	}
	// Unfortunately the other party has deleted the blob
	// in between Close and incRefCount.
	// The chunks we have written have already been
	// deleted at this point, so there's nothing we
	// can do except return an error. This situation
	// should be vanishingly unlikely in practice as
	// it relies on
	// a) two simultaneous initial uploads of the same blob.
	// b) one upload being removed immediately after upload.
	// c) the removal happening in the exact window between
	// f.Close and s.incRefCount.
	return false, fmt.Errorf("duplicate blob removed at an inopportune moment")
}

func (s *Storage) incRefCount(blobRef string) error {
	return s.fs.Files.Update(
		bson.D{{"filename", blobRef}},
		bson.D{{"$inc", bson.D{{"metadata.refcount", 1}}}},
	)
}

// Remove decrements the reference count of a blob and
// removes it if it is the last reference.
func (s *Storage) Remove(sha256Hash string) error {
	// The mgo gridfs interface does not allow us to atomically
	// remove a file, so we go behind the scenes to rename
	// the file if and only if the decremented reference count
	// is zero.
	blobRef := hashName(sha256Hash)
	change := mgo.Change{
		Update:    bson.D{{"$inc", bson.D{{"metadata.refcount", -1}}}},
		ReturnNew: true,
	}
	var doc struct {
		Metadata refCountMeta
	}
	_, err := s.fs.Files.Find(bson.D{{"filename", blobRef}}).Apply(change, &doc)
	if err != nil {
		return err
	}
	if doc.Metadata.RefCount != 0 {
		return nil
	}
	// The ref count has just reached zero. Rename the file atomically,
	// but only if the ref count has not been inremented in the meantime.
	newName := "deleted-" + bson.NewObjectId().Hex()
	err = s.fs.Files.Update(
		bson.D{{"filename", blobRef}, {"metadata.refcount", 0}},
		bson.D{{"$set", bson.D{{"filename", newName}}}},
	)
	if err == mgo.ErrNotFound {
		// Someone else must have got there first.
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot rename file before deletion: %v", err)
	}
	return s.fs.Remove(newName)
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

// Open opens the blob with the given hash.
// It returns mgo.ErrNotFound if the blob is not there.
func (s *Storage) Open(sha256hash string) (ReadSeekCloser, error) {
	f, err := s.fs.Open(hashName(sha256hash))
	if err != nil {
		return nil, err
	}
	return f, nil
}
