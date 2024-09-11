package eventbus

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
	_ "github.com/wxnacy/wgo/arrays"
	"golang.org/x/sync/semaphore"
)

type RedisEventBus struct {
	mu            sync.RWMutex
	redisClient   *redis.Client
	once          sync.Once
	subscribers   map[Topic][]SubscriberOptional
	errorHandler  ErrorHandler
	pollDuration  time.Duration
	handleTimeout time.Duration
}

func (eventBus *RedisEventBus) asyncDo(fn func()) {
	go func() {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			eventBus.errorHandler(ErrorOption{
				Title: "【事件总线】出现panic",
				Panic: r,
			})
		}()
		fn()
	}()
}
func (eventBus *RedisEventBus) asyncListenTopic(topic Topic) {
	eventBus.asyncDo(func() {
		key := GetRedisBlockListKey(topic)
		for {
			scores, err := eventBus.redisClient.BLPop(context.Background(), time.Second*30, key).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				eventBus.errorHandler(ErrorOption{
					Title: "【事件总线】监听队列出错",
					Topic: topic,
					Error: err,
				})
			}
			scoreMap := make(map[int64]struct{})
			for _, score := range scores {
				s := cast.ToInt64(score)
				_, ok := scoreMap[s]
				if ok {
					continue
				}
				scoreMap[s] = struct{}{}
				if s == 0 {
					eventBus.asyncPollTopic(topic)
				} else {
					eventBus.asyncDo(func() {
						t := TimestampToTime(s).Add(time.Millisecond * 300) //按照任务要执行的时间，延迟个300ms执行，避免一些临界情况，比如不同机器时间不一致
						deadline, cancelFunc := context.WithDeadline(context.Background(), t)
						defer cancelFunc()
						<-deadline.Done()
						eventBus.asyncPollTopic(topic)
					})
				}
			}

		}
	})
}
func (eventBus *RedisEventBus) asyncPollTopic(topics ...Topic) {
	if len(topics) == 0 {
		return
	}
	eventBus.asyncDo(func() {
		pipeline := eventBus.redisClient.Pipeline()
		cmdMap := make(map[Topic]*redis.StringSliceCmd)
		minScore := cast.ToString(0)
		maxScore := cast.ToString(GetIntTimestamp(time.Now()))
		//先按照时间拉取消息
		for _, topic := range topics {
			queueKey := GetRedisQueueKey(topic)
			cmdMap[topic] = pipeline.ZRangeByScore(context.Background(), queueKey, &redis.ZRangeBy{
				Min: minScore,
				Max: maxScore,
			})
		}
		_, err := pipeline.Exec(context.Background())
		if err != nil && !errors.Is(err, redis.Nil) {
			eventBus.errorHandler(ErrorOption{
				Title: "【事件总线】监听远程事件出现异常",
				Error: err,
			})
			return
		}
		topicMessageUidHandleMap := make(map[Topic]map[string]*redis.BoolCmd)
		//遍历拉取到的消息，并在缓存中setnx，只有setnx成功，才能处理，这里主要是为了防止多实例部署同时拉取到同一个消息并处理
		for topic, cmd := range cmdMap {
			messageUidList, err := cmd.Result()
			if err != nil {
				eventBus.errorHandler(ErrorOption{
					Title: "【事件总线】批量拉取主题消息出错",
					Error: err,
					Topic: topic,
				})
				continue
			}
			if len(messageUidList) == 0 {
				continue
			}
			for _, uid := range messageUidList {
				if topicMessageUidHandleMap[topic] == nil {
					topicMessageUidHandleMap[topic] = make(map[string]*redis.BoolCmd)
				}
				topicMessageUidHandleMap[topic][uid] = pipeline.SetNX(context.Background(), GetMessageHandleUid(uid), 1, RedisDefaultHandleTimeout)
			}
		}
		_, err = pipeline.Exec(context.Background())
		if err != nil {
			eventBus.errorHandler(ErrorOption{
				Title: "【事件总线】判断可否处理消息出错",
				Error: err,
			})
			return
		}
		for topic, messageUidHandleMap := range topicMessageUidHandleMap {
			messageUidList := make([]string, 0, len(messageUidHandleMap))
			for messageUid, setNxCmd := range messageUidHandleMap {
				setNx, err := setNxCmd.Result()
				if err != nil {
					eventBus.errorHandler(ErrorOption{
						Title: "【事件总线】抢占消息处理出错",
						Error: err,
						Topic: topic,
					})
					continue
				}
				if !setNx {
					continue
				}
				messageUidList = append(messageUidList, messageUid)
			}
			if len(messageUidList) == 0 {
				continue
			}
			//投放当前实例在缓存中抢占到要处理的消息
			eventBus.asyncPollMessage(topic, messageUidList)
		}
	})
}
func (eventBus *RedisEventBus) getRemoteMessage(uidList []string) map[string]RemoteMessage {
	messages := make(map[string]RemoteMessage)
	if len(uidList) == 0 {
		return messages
	}
	messageCmdMap := make(map[string]*redis.StringCmd)
	optionalCmdMap := make(map[string]*redis.StringCmd)
	executeTimesCmdMap := make(map[string]*redis.StringCmd)
	pipeline := eventBus.redisClient.Pipeline()
	for _, uid := range uidList {
		messageCmdMap[uid] = pipeline.Get(context.Background(), GetMessageUid(uid))
		optionalCmdMap[uid] = pipeline.Get(context.Background(), GetPublishOptionalUid(uid))
		executeTimesCmdMap[uid] = pipeline.Get(context.Background(), GetMessageExecuteTimesUid(uid))
	}
	_, err := pipeline.Exec(context.Background())
	if err != nil && !errors.Is(err, redis.Nil) {
		return messages
	}
	for _, uid := range uidList {
		msgCmd := messageCmdMap[uid]
		msgBytes, err := msgCmd.Bytes()
		if err != nil {
			continue
		}
		optionalCmd := optionalCmdMap[uid]
		optionalBytes, err := optionalCmd.Bytes()
		publishOptional := PublishOptional{}
		if err == nil {
			if err = jsoniter.Unmarshal(optionalBytes, &publishOptional); err != nil {
				publishOptional = GetDefaultPublishOptional()
			}
		} else {
			publishOptional = GetDefaultPublishOptional()
		}
		executeCmd := executeTimesCmdMap[uid]
		executeTimes, _ := executeCmd.Int()
		messages[uid] = RemoteMessage{
			Message:      msgBytes,
			Optional:     publishOptional,
			ExecuteTimes: executeTimes,
		}
	}
	return messages
}
func (eventBus *RedisEventBus) getSubscribers(topic Topic) []SubscriberOptional {
	eventBus.mu.RLock()
	defer eventBus.mu.RUnlock()
	return CopySubscriberOptionals(eventBus.subscribers[topic])
}
func (eventBus *RedisEventBus) handleQueueMessage(topic Topic, uid string, message []byte) error {
	defer func() {
		if p := recover(); p != nil {
			eventBus.errorHandler(ErrorOption{
				Title:   "【事件总线】处理消息出现异常",
				Topic:   topic,
				Uid:     uid,
				Message: string(message),
				Panic:   p,
			})
		}
	}()
	ctx := WithEventBus(nil)
	subscribers := eventBus.getSubscribers(topic)
	for _, sub := range subscribers {
		data, err := sub.decode(message)
		if err != nil {
			return err
		}
		err = sub.subscriber.Handle(ctx, data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (eventBus *RedisEventBus) asyncPollMessage(topic Topic, messageUidList []string) {
	eventBus.asyncDo(func() {
		if len(messageUidList) == 0 {
			return
		}
		doneMsgUidMap := make(map[string]struct{})
		defer func() {

			pipe := eventBus.redisClient.Pipeline()
			for _, messageUid := range messageUidList {
				_, ok := doneMsgUidMap[messageUid]
				if ok { //如果是处理完成的消息，在Redis的执行权限则放着自动过期，避免多进程拉取消息时，拉到已经处理完成的消息，如果是处理失败的消息，则删除执行权，等待下次执行
					continue
				}
				pipe.Del(context.Background(), GetMessageHandleUid(messageUid))
			}
			_, err := pipe.Exec(context.Background())
			if err != nil {
				eventBus.errorHandler(ErrorOption{
					Title: "【事件总线】释放消息处理权限出错",
					Error: err,
					Topic: topic,
				})
			}
		}()
		remoteMessage := eventBus.getRemoteMessage(messageUidList)
		num := int64(len(messageUidList))
		weight := semaphore.NewWeighted(num)
		ok := weight.TryAcquire(num)
		if !ok {
			eventBus.errorHandler(ErrorOption{
				Title: "【事件总线】获取信号量出错",
				Error: fmt.Errorf("获取信号量出错,n:%d", num),
			})
			return
		}
		doneMessageChan := make(chan DoneMessage, num)
		for _, messageUid := range messageUidList {
			uid := messageUid
			eventBus.asyncDo(func() {
				defer weight.Release(1)
				message, ok := remoteMessage[uid]
				if !ok {
					eventBus.errorHandler(ErrorOption{
						Title: "【事件总线】获取不到消息内容",
						Topic: topic,
						Uid:   uid,
					})
					return
				}
				err := eventBus.handleQueueMessage(topic, uid, message.Message)
				doneMessageChan <- DoneMessage{
					Uid:   uid,
					Error: err,
				}

			})
		}
		ctx, cancel := context.WithTimeout(context.Background(), eventBus.handleTimeout)
		defer cancel()
		_ = weight.Acquire(ctx, num)
		close(doneMessageChan)
		doneMessageMap := make(map[string]DoneMessage)
		pipeline := eventBus.redisClient.Pipeline()
		for msg := range doneMessageChan {
			doneMessageMap[msg.Uid] = msg
			if msg.Error == nil {
				doneMsgUidMap[msg.Uid] = struct{}{}
				//如果处理消息成功，则将消息从有序集合移除，包括消息的参数和执行选项
				pipeline.Del(context.Background(), GetMessageUid(msg.Uid))
				pipeline.Del(context.Background(), GetPublishOptionalUid(msg.Uid))
				pipeline.ZRem(context.Background(), GetRedisQueueKey(topic), msg.Uid)
			}

		}
		retryUidList := make([]string, 0)
		for _, uid := range messageUidList {
			message := remoteMessage[uid]
			msg, ok := doneMessageMap[uid]
			if !ok {
				retryUidList = append(retryUidList, uid)
				eventBus.errorHandler(ErrorOption{
					Title:        "【事件总线】处理消息超时",
					Topic:        topic,
					Uid:          uid,
					Message:      string(message.Message),
					ExecuteTimes: message.ExecuteTimes,
				})
			}
			if msg.Error != nil {
				retryUidList = append(retryUidList, uid)
				eventBus.errorHandler(ErrorOption{
					Title:        "【事件总线】处理消息出现错误",
					Error:        msg.Error,
					Topic:        topic,
					Uid:          uid,
					Message:      string(message.Message),
					ExecuteTimes: message.ExecuteTimes,
				})
			}
		}
		for _, uid := range retryUidList { //这里都是处理失败或处理超时的uid
			optional := GetDefaultPublishOptional()
			message, ok := remoteMessage[uid]
			if ok {
				optional = message.Optional
			} else {
				eventBus.errorHandler(ErrorOption{
					Title: "【事件总线】获取不到事件推送参数",
					Topic: topic,
					Uid:   uid,
				})
			}
			if nextTime, ok := GetNextTryTime(message.ExecuteTimes, optional); ok {
				pipeline.Expire(context.Background(), GetMessageUid(uid), RedisMessageTimeout)
				pipeline.Expire(context.Background(), GetPublishOptionalUid(uid), RedisMessageTimeout)
				pipeline.ZAdd(context.Background(), GetRedisQueueKey(topic), &redis.Z{
					Score:  GetFloatTimestamp(nextTime),
					Member: uid,
				})
				pipeline.Incr(context.Background(), GetMessageExecuteTimesUid(uid))
			} else {
				eventBus.errorHandler(ErrorOption{
					Title:   "【事件总线】消息达到最大失败处理次数",
					Topic:   topic,
					Uid:     uid,
					Message: string(remoteMessage[uid].Message),
				})
				//如果消息已经达到最大处理次数，也从消息队列移除
				pipeline.ZRem(context.Background(), GetRedisQueueKey(topic), uid)
				pipeline.Del(context.Background(), GetMessageUid(uid))
				pipeline.Del(context.Background(), GetPublishOptionalUid(uid))
				pipeline.Del(context.Background(), GetMessageExecuteTimesUid(uid))
			}
		}
		if _, err := pipeline.Exec(context.Background()); err != nil {
			eventBus.errorHandler(ErrorOption{
				Title: "【事件总线】消息处理完毕后执行Redis管道命令出错",
				Error: err,
			})
		}

	})
}

func (eventBus *RedisEventBus) getAllTopic() []Topic {
	eventBus.mu.Lock()
	defer eventBus.mu.Unlock()
	result := make([]Topic, 0, len(eventBus.subscribers))
	for topic := range eventBus.subscribers {
		result = append(result, topic)
	}
	return result
}
func (eventBus *RedisEventBus) asyncLoopTopic() {
	eventBus.once.Do(func() {
		eventBus.asyncDo(func() {
			for {
				allTopic := eventBus.getAllTopic()
				eventBus.asyncPollTopic(allTopic...)
				time.Sleep(eventBus.pollDuration)
			}
		})
	})
}
func (eventBus *RedisEventBus) Register(subscriber Subscriber, options ...SubscriberOption) error {
	if subscriber == nil {
		return ErrSubscriberNil
	}
	event := subscriber.Event()
	topic := event.Topic
	data := event.Data
	if len(topic.String()) == 0 {
		return ErrTopicEmpty
	}
	optional := BuildSubscriberOptional(subscriber, options...)
	if optional.decode == nil { //如果没有传默认的解码，则默认认为data是结构体
		typeOf := reflect.TypeOf(data)
		if reflect.TypeOf(data).Kind() != reflect.Struct {
			return ErrEventDataMustStruct
		}
		optional.decode = func(dataBytes []byte) (interface{}, error) {
			value := reflect.New(typeOf)
			err := jsoniter.Unmarshal(dataBytes, value.Interface())
			if err != nil {
				return nil, err
			}
			return value.Elem().Interface(), nil
		}
	}
	eventBus.mu.Lock()
	defer eventBus.mu.Unlock()
	if eventBus.subscribers == nil {
		eventBus.subscribers = make(map[Topic][]SubscriberOptional)
	}
	if eventBus.subscribers[topic] == nil {
		eventBus.subscribers[topic] = make([]SubscriberOptional, 0)
	}
	//如果该主题是第一次有订阅者，则为这个主题创建一个协程监听事件
	if len(eventBus.subscribers[topic]) == 0 {
		eventBus.asyncListenTopic(topic)
	}
	eventBus.asyncLoopTopic()
	eventBus.subscribers[topic] = append(eventBus.subscribers[topic], optional)
	SortSubscriberOptionals(eventBus.subscribers[topic])
	return nil
}

func (eventBus *RedisEventBus) Publish(ctx context.Context, event Event, options ...PublishOption) error {
	topic := event.Topic
	data := event.Data
	if len(topic.String()) == 0 {
		return ErrTopicEmpty
	}
	publishOptional := GetDefaultPublishOptional()
	if len(options) != 0 {
		for _, opt := range options {
			if opt != nil {
				opt(&publishOptional)
			}
		}
	}
	if publishOptional.Encode == nil {
		publishOptional.Encode = func(v interface{}) ([]byte, error) {
			return jsoniter.Marshal(v)
		}
	}
	optionalByes, err := jsoniter.Marshal(publishOptional)
	if err != nil {
		return err
	}
	dataBytes, err := publishOptional.Encode(data)
	if err != nil {
		return err
	}
	uid := uuid.New().String()
	queueKey := GetRedisQueueKey(topic)
	var score float64
	pipeline := eventBus.redisClient.Pipeline()
	if publishOptional.LeastDelay > 0 {
		score = GetFloatTimestamp(time.Now().Add(publishOptional.LeastDelay))
	}
	pipeline.LPush(ctx, GetRedisBlockListKey(topic), score)
	pipeline.ZAdd(ctx, queueKey, &redis.Z{
		Score:  score,
		Member: uid,
	})
	pipeline.SAdd(ctx, RedisEventRemoteSet, topic.String())
	pipeline.Expire(ctx, queueKey, RedisMessageTimeout)
	pipeline.Set(ctx, GetMessageUid(uid), dataBytes, RedisMessageTimeout)
	pipeline.Set(ctx, GetPublishOptionalUid(uid), optionalByes, RedisMessageTimeout)
	pipeline.Set(ctx, GetMessageExecuteTimesUid(uid), 0, RedisMessageTimeout)
	if _, err = pipeline.Exec(ctx); err != nil {
		return err
	}
	return nil
}

func (eventBus *RedisEventBus) GetTopicInfo() ([]TopicInfo, error) {
	result := make([]TopicInfo, 0)
	topics, err := eventBus.redisClient.SMembers(context.Background(), RedisEventRemoteSet).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return result, err
	}
	if len(topics) == 0 {
		return result, nil
	}
	pipeline := eventBus.redisClient.Pipeline()
	cmdMap := make(map[string]*redis.IntCmd)
	for _, topic := range topics {
		queueKey := GetRedisQueueKey(Topic(topic))
		cmdMap[topic] = pipeline.ZCard(context.Background(), queueKey)
	}
	_, err = pipeline.Exec(context.Background())
	if err != nil && !errors.Is(err, redis.Nil) {
		return result, err
	}
	for topic, cmd := range cmdMap {
		count, err := cmd.Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return result, err
		}
		result = append(result, TopicInfo{
			Topic:        Topic(topic),
			MessageCount: count,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].MessageCount > result[j].MessageCount
	})
	return result, nil
}

func NewRedisEventBus(redisClient *redis.Client, options ...EventBusOption) *RedisEventBus {
	eventBus := &RedisEventBus{
		redisClient:   redisClient,
		once:          sync.Once{},
		subscribers:   nil,
		errorHandler:  nil,
		pollDuration:  0,
		handleTimeout: 0,
	}
	optional := EventBusOptional{}
	if len(options) != 0 {
		for _, opt := range options {
			if opt != nil {
				opt(&optional)
			}
		}
	}
	eventBus.errorHandler = optional.errorHandler
	if eventBus.errorHandler == nil {
		eventBus.errorHandler = func(option ErrorOption) {

		}
	}
	eventBus.pollDuration = optional.pollDuration
	if eventBus.pollDuration == 0 {
		eventBus.pollDuration = RedisDefaultPollDuration
	}
	if eventBus.pollDuration < RedisMinPollDuration {
		eventBus.pollDuration = RedisMinPollDuration
	}

	eventBus.handleTimeout = optional.handleTimeout
	if eventBus.handleTimeout == 0 {
		eventBus.handleTimeout = RedisDefaultHandleTimeout
	}
	if eventBus.handleTimeout < RedisMinHandleTimeout {
		eventBus.handleTimeout = RedisMinHandleTimeout
	}
	var _ EventBus = eventBus
	return eventBus
}
