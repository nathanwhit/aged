package orchestrator

import (
	"sync"

	"aged/internal/core"
)

type Broker struct {
	mu          sync.Mutex
	nextID      int
	subscribers map[int]chan core.Event
}

func NewBroker() *Broker {
	return &Broker{subscribers: map[int]chan core.Event{}}
}

func (b *Broker) Subscribe() (int, <-chan core.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := b.nextID
	b.nextID++
	ch := make(chan core.Event, 128)
	b.subscribers[id] = ch
	return id, ch
}

func (b *Broker) Unsubscribe(id int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ch, ok := b.subscribers[id]; ok {
		delete(b.subscribers, id)
		close(ch)
	}
}

func (b *Broker) Publish(event core.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, ch := range b.subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}
