package eventbus

import "context"

type Subscriber interface {
	Event() Event
	Handle(ctx context.Context, data interface{}) error
}
