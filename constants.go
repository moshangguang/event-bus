package eventbus

import "time"

// common
const (
	CtxEventBusLoopTimes = "ctx_event_bus_loop_times"
)

// redis
const (
	RedisEventBlockList           = "redis_event_block_list"
	RedisEventTopicQueue          = "redis_event_topic_queue"
	RedisEventMessageUid          = "redis_event_message_uid"
	RedisEventPublishOptionalUid  = "redis_event_publish_optional_uid"
	RedisEventMessageHandleUid    = "redis_event_message_handle_uid"
	RedisEventMessageExecuteTimes = "redis_event_message_execute_times"
	RedisEventRemoteSet           = "redis_event_remote_set"
	RedisMessageTimeout           = time.Hour * 24 * 30
	RedisMinInterval              = time.Millisecond * 100
	RedisDefaultInterval          = time.Second * 30
	RedisMaxInterval              = time.Minute * 5
	RedisMinDuration              = time.Minute
	RedisMaxDuration              = time.Hour * 2
	RedisMinPollDuration          = time.Second
	RedisDefaultPollDuration      = time.Second * 3
	RedisMinHandleTimeout         = 10 * time.Second
	RedisDefaultHandleTimeout     = 10 * time.Minute
	MinWarnGoCount                = 128
	DefaultWarnGoCount            = 1024
	MinWarnGoCountInterval        = time.Second * 10
	DefaultWarnGoCountInterval    = time.Second * 30
	MinOverflowTimeout            = time.Minute * 5
	DefaultOverflowTimeout        = time.Hour
	MinOverflowInterval           = time.Second * 10
	DefaultOverflowInterval       = time.Hour
)
