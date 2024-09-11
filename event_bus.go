package eventbus

import (
	"context"
)

type EventBus interface {
	Register(subscriber Subscriber, options ...SubscriberOption) error
	Publish(ctx context.Context, event Event, options ...PublishOption) error
}
