package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/go-redis/redis/v8"

	eventbus "github.com/moshangguang/event-bus"
)

// <1>定义远程消息的topic
const RedisTopic = "redis_topic"

type RedisTopicData struct { //<2>定义远程消息的类型
	Name string
	Age  int
}

func GetRedisClient() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       6,
	})
	ping := client.Ping(context.Background())
	if err := ping.Err(); err != nil {
		return nil, err
	}
	return client, nil
}

func main() {
	client, err := GetRedisClient()
	if err != nil {
		panic(err)
	}
	//<3>定义错误处理器
	//<4>创建一个Redis事件总线
	eventBus := eventbus.NewRedisEventBus(client, eventbus.WithErrHandlerOption(func(option eventbus.ErrorOption) {

	}))
	//<5>注册订阅者
	if err := eventBus.Register(RedisEventSubscriber{}); err != nil {
		panic(err)
	}
	ctx := eventbus.WithEventBus(context.Background())
	//<6>推送事件，立即执行
	err = eventBus.Publish(ctx, eventbus.Event{
		Topic: RedisTopic,
		Data: RedisTopicData{
			Name: "John",
			Age:  20,
		},
	})
	if err != nil {
		panic(err)
	}
	//<7>推送事件，消息至少延迟5s后再执行
	err = eventBus.Publish(ctx, eventbus.Event{
		Topic: RedisTopic,
		Data: RedisTopicData{
			Name: "Amy",
			Age:  19,
		},
	}, eventbus.WithLeastDelayOption(5*time.Second))
	if err != nil {
		panic(err)
	}
	time.Sleep(30 * time.Second)
}

type RedisEventSubscriber struct {
}

func (RedisEventSubscriber) Event() eventbus.Event {
	return eventbus.Event{
		Topic: RedisTopic,
		Data:  RedisTopicData{},
	}
}
func (RedisEventSubscriber) Handle(ctx context.Context, data interface{}) error {
	d, ok := data.(RedisTopicData)
	if !ok {
		return nil
	}
	fmt.Println("收到事件", d)
	return nil
}
