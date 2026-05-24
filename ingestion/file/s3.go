package file

type S3Adapter struct {
	bucket string
	events chan RawEvent
}

func (s *S3Adapter) Start() error {
}

func (s *S3Adapter) Events() <-chan RawEvent { return s.events }
