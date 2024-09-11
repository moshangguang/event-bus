package eventbus

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
)

type LocalEventBus struct {
	mu          sync.RWMutex
	subscribers map[Topic][]SubscriberOptional
}

func (eventBus *LocalEventBus) Register(subscriber Subscriber, options ...SubscriberOption) error {
	if subscriber == nil {
		return ErrSubscriberNil
	}
	optional := BuildSubscriberOptional(subscriber, options...)
	topic := subscriber.Event().Topic
	if len(topic.String()) == 0 {
		return ErrTopicEmpty
	}
	eventBus.mu.Lock()
	defer eventBus.mu.Unlock()
	if eventBus.subscribers == nil {
		eventBus.subscribers = make(map[Topic][]SubscriberOptional)
	}
	if eventBus.subscribers[topic] == nil {
		eventBus.subscribers[topic] = make([]SubscriberOptional, 0)
	}
	eventBus.subscribers[topic] = append(eventBus.subscribers[topic], optional)
	SortSubscriberOptionals(eventBus.subscribers[topic])
	return nil
}
func (eventBus *LocalEventBus) getSubscribers(topic Topic) []SubscriberOptional {
	eventBus.mu.RLock()
	defer eventBus.mu.RUnlock()
	return CopySubscriberOptionals(eventBus.subscribers[topic])
}
func (eventBus *LocalEventBus) Publish(ctx context.Context, event Event, options ...PublishOption) error {
	if len(options) != 0 {
		return ErrNotSupportOptions
	}
	topic := event.Topic
	data := event.Data
	if len(topic.String()) == 0 {
		return ErrTopicEmpty
	}
	times, err := MustGetLoopTimes(ctx)
	if err != nil {
		return err
	}
	if atomic.LoadInt32(times) >= math.MaxUint16 {
		return ErrEventLoopOverflow
	}
	atomic.AddInt32(times, 1)
	subscriberOptionals := eventBus.getSubscribers(topic)
	for _, optional := range subscriberOptionals {
		err = optional.subscriber.Handle(ctx, data)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewLocalEventBus() *LocalEventBus {
	eventBus := new(LocalEventBus)
	var _ EventBus = eventBus
	return eventBus
}
