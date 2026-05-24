package file

type S3Adapter struct {
	bucket string
	events chan RawEvent
}

func NewS3Adapter(bucket string) *S3Adapter {
	return &S3Adapter{
		bucket: bucket,
		events: make(chan RawEvent, 100), // Buffered channel to hold events
	}
}

func (s *S3Adapter) Start() error {
	return nil
}

func (s *S3Adapter) Events() <-chan RawEvent { return s.events }
