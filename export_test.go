package blobstore

func (s *Storage) IncrementGCGeneration() (int64, error) {
	return s.incrementGCGeneration()
}
