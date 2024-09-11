package eventbus

import (
	"context"
	"fmt"
	_ "github.com/go-redis/redis/v8"
	"sort"
	"time"
)

func GetRedisBlockListKey(topic Topic) string {
	return fmt.Sprintf("%s:%s", RedisEventBlockList, topic)
}
func GetRedisQueueKey(topic Topic) string {
	return fmt.Sprintf("%s:%s", RedisEventTopicQueue, topic)
}
func GetMessageUid(uid string) string {
	return fmt.Sprintf("%s:%s", RedisEventMessageUid, uid)
}
func GetPublishOptionalUid(uid string) string {
	return fmt.Sprintf("%s:%s", RedisEventPublishOptionalUid, uid)
}

func GetMessageHandleUid(uid string) string {
	return fmt.Sprintf("%s:%s", RedisEventMessageHandleUid, uid)
}
func GetMessageExecuteTimesUid(uid string) string {
	return fmt.Sprintf("%s:%s", RedisEventMessageExecuteTimes, uid)
}
func GetIntTimestamp(t time.Time) int64 {
	return t.UnixMilli()
}
func GetFloatTimestamp(t time.Time) float64 {
	return float64(GetIntTimestamp(t))
}
func TimestampToTime(timestamp int64) time.Time {
	return time.UnixMilli(timestamp)
}
func GetNextTryTime(executeTimes int, optional PublishOptional) (time.Time, bool) {
	if optional.TryTimes != 0 && executeTimes >= optional.TryTimes {
		return time.Time{}, false
	}
	add := optional.Interval
	if add < RedisMinInterval || add > RedisMaxInterval {
		add = RedisMinInterval
	}
	max := optional.MaxDuration
	if max < RedisMinDuration || max > RedisMaxDuration {
		max = RedisMaxDuration
	}
	for i := 0; i < executeTimes; i++ {
		add *= 2
		if add > RedisMaxDuration {
			add = RedisMaxDuration
			break
		}
	}
	return time.Now().Add(add), true

}

func GetDefaultPublishOptional() PublishOptional {
	return PublishOptional{
		Interval:    RedisDefaultInterval,
		MaxDuration: time.Hour,
		TryTimes:    0,
	}
}

func WithEventBus(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	loopTimes := ctx.Value(CtxEventBusLoopTimes)
	if loopTimes != nil {
		if _, ok := loopTimes.(*int32); ok {
			return ctx
		}
	}
	return context.WithValue(ctx, CtxEventBusLoopTimes, new(int32))
}
func MustGetLoopTimes(ctx context.Context) (*int32, error) {
	if ctx == nil {
		return nil, ErrCtxNil
	}
	times := ctx.Value(CtxEventBusLoopTimes)
	if times == nil {
		return nil, ErrCtxNotFoundLoopTimes
	}
	t, ok := times.(*int32)
	if ok {
		return t, nil
	}
	return nil, ErrCtxLoopTimesType
}

func BuildSubscriberOptional(subscriber Subscriber, option ...SubscriberOption) SubscriberOptional {
	optional := SubscriberOptional{
		subscriber: subscriber,
	}
	if len(option) != 0 {
		for _, opt := range option {
			if opt != nil {
				opt(&optional)
			}
		}
	}
	return optional
}

func SortSubscriberOptionals(optional []SubscriberOptional) {
	if len(optional) <= 1 {
		return
	}
	sort.Slice(optional, func(i, j int) bool {
		return optional[i].order < optional[j].order
	})
}

func CopySubscriberOptionals(optional []SubscriberOptional) []SubscriberOptional {
	result := make([]SubscriberOptional, 0, len(optional))
	result = append(result, optional...)
	return result
}
