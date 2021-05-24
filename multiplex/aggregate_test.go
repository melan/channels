package multiplex

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Aggregate(t *testing.T) {
	tests := []struct {
		name           string
		inChannels     []chan string
		loadFunc       func(ctx context.Context, inChannels []chan string)
		outChannelSize int
		wantOutput     []string
	}{
		{
			name:       "no input channels",
			inChannels: nil,
			loadFunc:   func(ctx context.Context, inChannels []chan string) {},
			wantOutput: nil,
		},
		{
			name:       "one channel 5 messages",
			inChannels: []chan string{make(chan string, 1)},
			loadFunc: func(ctx context.Context, inChannels []chan string) {
				for i := 0; i < 5; i++ {
					select {
					case <-ctx.Done():
						return
					default:
						inChannels[0] <- strconv.Itoa(i)
					}
				}
				close(inChannels[0])
			},
			wantOutput: []string{"0", "1", "2", "3", "4"},
		},
		{
			name: "five channels, one message",
			inChannels: []chan string{
				make(chan string, 1),
				make(chan string, 1),
				make(chan string, 1),
				make(chan string, 1),
				make(chan string, 1),
			},
			loadFunc: func(ctx context.Context, inChannels []chan string) {
				for i := 0; i < 5; i++ {
					select {
					case <-ctx.Done():
						return
					default:
						inChannels[i] <- strconv.Itoa(i)
						close(inChannels[i])
					}
				}
			},
			wantOutput: []string{"0", "1", "2", "3", "4"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Log("creating context")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go tt.loadFunc(ctx, tt.inChannels)

			got := Aggregate(ctx, tt.outChannelSize, tt.inChannels...)

			if (got == nil) != (tt.wantOutput == nil) {
				t.Errorf("both actual or expected is expected to be nil, or not but one of them isn't. Expected: %+v, actual: %+v", tt.wantOutput, got)
			}

			if got == nil {
				return
			}

			var events []string
			timer := time.After(5 * time.Second)
		CONSUMEEVENTS:
			for {
				select {
				case msg, ok := <-got:
					if !ok {
						t.Log("aggregated channel is closed")
						break CONSUMEEVENTS
					}

					events = append(events, msg)
				case <-timer:
					t.Log("time is over")
					break CONSUMEEVENTS
				}
			}

			assert.ElementsMatch(t, tt.wantOutput, events)
		})
	}
}

func Benchmark_Aggregate(b *testing.B) {
	inChannels := make([]chan string, 0, 100)
	for i := 0; i < 100; i++ {
		inChannels = append(inChannels, make(chan string, 1))
	}

	messages := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		messages = append(messages, strconv.Itoa(i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	outChannel := Aggregate(ctx, 20, inChannels...)

	postedResults := make(map[int]int)
	go func() {
		for i := 0; i < 100000; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				channel := inChannels[rand.Intn(100)]
				messageID := rand.Intn(100)
				postedResults[messageID]++
				channel <- messages[messageID]
			}
		}

		for i := 0; i < 100; i++ {
			close(inChannels[i])
		}
	}()

	consumedResults := make(map[int]int)

CONSUME:
	for {
		select {
		case <-ctx.Done():
			b.Log("timeout")
			break CONSUME
		case msg, ok := <-outChannel:
			if !ok {
				b.Log("out channel is closed")
				break CONSUME
			}

			messageID, err := strconv.Atoi(msg)
			if err != nil {
				b.Errorf("unexpected error in the message conversion %q: %w", msg, err)
				return
			}
			consumedResults[messageID]++
		}
	}

	if len(postedResults) != len(consumedResults) {
		b.Errorf("length of posted and consumed maps doesn't match: posted: %d, consumed: %d", len(postedResults), len(consumedResults))
	}

	for idx, cnt := range consumedResults {
		postedCnt, found := postedResults[idx]
		if !found {
			b.Errorf("message %d was consumed but wasn't posted", idx)
		}

		if cnt != postedCnt {
			b.Errorf("Posted %d and consumed %d counters don't match for the message %d", postedCnt, cnt, idx)
		}
	}
}
