package multiplex

import "context"

func Aggregate(ctx context.Context, outChannelSize int, inChan ...chan string) chan string {
	if outChannelSize < 0 {
		outChannelSize = 0
	}

	switch len(inChan) {
	case 0:
		return nil
	case 1:
		return passChan(ctx, inChan[0], outChannelSize)
	default:
		prevOutChannel := inChan[0]
		for i := 1; i < len(inChan); i++ {
			prevOutChannel = orChan(ctx, prevOutChannel, inChan[i], 1)
		}

		return passChan(ctx, prevOutChannel, outChannelSize)
	}
}

func passChan(ctx context.Context, inChannel chan string, outChanSize int) chan string {
	if outChanSize < 0 {
		outChanSize = 0
	}

	outChan := make(chan string, outChanSize)

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(outChan)
				return
			case v, ok := <-inChannel:
				if !ok {
					close(outChan)
					return
				}

				outChan <- v
			}
		}
	}()
	return outChan
}

func orChan(ctx context.Context, channel1, channel2 chan string, outChanSize int) chan string {
	if outChanSize < 0 {
		outChanSize = 0
	}

	outChan := make(chan string, outChanSize)

	go func() {
		var channel1Closed, channel2Closed bool
		for {
			if channel1Closed && channel2Closed {
				close(outChan)
				return
			}

			select {
			case <-ctx.Done():
				close(outChan)
				return
			case v, ok := <-channel1:
				if !ok {
					channel1Closed = true
					continue
				}

				outChan <- v
			case v, ok := <-channel2:
				if !ok {
					channel2Closed = true
					continue
				}

				outChan <- v
			}
		}
	}()
	return outChan
}
