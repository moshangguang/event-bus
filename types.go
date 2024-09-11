package eventbus

import (
	"time"
)

type Topic string

func (topic Topic) String() string {
	return string(topic)
}

type Event struct {
	Topic Topic
	Data  interface{}
}

type EventBusOptional struct {
	errorHandler  func(option ErrorOption)
	pollDuration  time.Duration
	handleTimeout time.Duration
}

type PublishOption func(*PublishOptional)
type PublishOptional struct {
	Interval    time.Duration                       `json:"interval"`
	MaxDuration time.Duration                       `json:"max_duration"`
	TryTimes    int                                 `json:"try_times"`
	LeastDelay  time.Duration                       `json:"least_delay"`
	Encode      func(v interface{}) ([]byte, error) `json:"-"`
}

func WithInternalOption(interval time.Duration) PublishOption {
	return func(optional *PublishOptional) {
		if interval <= RedisMinInterval {
			interval = RedisMinInterval
		}
		optional.Interval = interval
	}
}

func WithMaxDurationOption(maxDuration time.Duration) PublishOption {
	return func(optional *PublishOptional) {
		if maxDuration <= RedisMinDuration {
			maxDuration = RedisMinDuration
		}
		optional.MaxDuration = maxDuration
	}
}
func WithTryTimesOption(tryTimes int) PublishOption {
	return func(optional *PublishOptional) {
		if tryTimes < 0 {
			tryTimes = 0
		}
		optional.TryTimes = tryTimes
	}
}

func WithLeastDelayOption(leastDelay time.Duration) PublishOption {
	return func(optional *PublishOptional) {
		optional.LeastDelay = leastDelay
	}
}

func WithEncodeOption(encode func(v interface{}) ([]byte, error)) PublishOption {
	if encode == nil {
		panic("encode is nil")
	}
	return func(optional *PublishOptional) {
		optional.Encode = encode
	}
}

type SubscriberOption func(*SubscriberOptional)

func WithOrderOption(order int) SubscriberOption {
	return func(optional *SubscriberOptional) {
		optional.order = order
	}
}
func WithDecodeOption(decode func([]byte) (interface{}, error)) SubscriberOption {
	if decode == nil {
		panic("decode is nil")
	}
	return func(optional *SubscriberOptional) {
		optional.decode = decode
	}
}

type SubscriberOptional struct {
	subscriber Subscriber
	decode     func([]byte) (interface{}, error)
	order      int
}

type DoneMessage struct {
	Uid   string
	Error error
}
type ErrorOption struct {
	Title        string
	Topic        Topic
	Uid          string
	Message      string
	ExecuteTimes int
	Panic        interface{}
	Error        error
	Topics       []Topic
}
type RemoteMessage struct {
	Message      []byte
	Optional     PublishOptional
	ExecuteTimes int
}

type EventBusOption func(optional *EventBusOptional)

type ErrorHandler func(option ErrorOption)

func WithHandleTimeout(timeout time.Duration) EventBusOption {
	return func(optional *EventBusOptional) {
		optional.handleTimeout = timeout
	}
}

func WithErrHandlerOption(errorHandler ErrorHandler) EventBusOption {
	if errorHandler == nil {
		panic("err handler is nil")
	}
	return func(optional *EventBusOptional) {
		optional.errorHandler = errorHandler
	}
}

func WithPollDurationOption(pollDuration time.Duration) EventBusOption {
	return func(optional *EventBusOptional) {
		optional.pollDuration = pollDuration
	}
}

type TopicInfo struct {
	Topic        Topic
	MessageCount int64
}
