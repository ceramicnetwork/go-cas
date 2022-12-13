package batcher

type StreamBatcher struct {
}

func NewStreamBatcher() *StreamBatcher {
	return &StreamBatcher{}
}

func (b StreamBatcher) Batch() {
	//cfg, err := aws.Config()
	//if err != nil {
	//	log.Fatalf("failed to create aws cfg: %v", err)
	//}
	//db := aws.NewDynamoDb(cfg)

	for {
		// 1. Read from Ready queue
		// 2. Add request to batch
		// 3. Check batch finalization conditions
		// 4. Post completed batches to Worker queue.
		func() {
		}()
		// TODO
		//time.Sleep(time.Minute)
		return
	}
}
