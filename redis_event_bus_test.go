package eventbus

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func GetRedisClient() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       3,
	})
	ping := client.Ping(context.Background())
	if err := ping.Err(); err != nil {
		return nil, err
	}
	return client, nil
}

func TestRedisEventBus_Publish(t *testing.T) {
	client, err := GetRedisClient()
	if err != nil {
		t.Errorf("创建Redis客户端出错,err:%s", err.Error())
		return
	}
	eventBus := NewRedisEventBus(client)
	err = eventBus.Register(RedisSubscriber1{
		times: new(int32),
	})
	if err != nil {
		t.Errorf("注册远程观察者出错,err:%s", err.Error())
		return
	}
	err = eventBus.Publish(context.Background(), Event{
		Topic: RedisTopic,
		Data: RedisData1{
			Name: "Amy",
			Age:  20,
		},
	}, WithInternalOption(1*time.Second))
	if err != nil {
		t.Errorf("推送事件出错,err:%s", err.Error())
		return
	}
	err = eventBus.Publish(context.Background(), Event{
		Topic: RedisTopic,
		Data: RedisData1{
			Name: "John",
			Age:  15,
		},
	}, WithLeastDelayOption(5*time.Second), WithInternalOption(1*time.Second))
	if err != nil {
		t.Errorf("推送事件出错,err:%s", err.Error())
		return
	}
	time.Sleep(60 * time.Second)
}

type RedisSubscriber1 struct {
	times *int32
}

const RedisTopic = "redis_topic5"

type RedisData1 struct {
	Name string
	Age  int
}

func (RedisSubscriber1) Event() Event {
	return Event{
		Topic: RedisTopic,
		Data:  RedisData1{},
	}
}
func (sub RedisSubscriber1) Handle(ctx context.Context, data interface{}) error {
	d := data.(RedisData1)

	times := atomic.AddInt32(sub.times, 1)
	if times%3 != 0 {
		fmt.Println("收到事件，出错", d, time.Now())
		return fmt.Errorf("666")
	} else {
		fmt.Println("收到事件", d, time.Now())
		return nil
	}
}

func TestRedisEventBus_Publish2(t *testing.T) {
	client, err := GetRedisClient()
	if err != nil {
		t.Errorf("创建Redis客户端出错,err:%s", err.Error())
		return
	}
	eventBus := NewRedisEventBus(client)
	err = eventBus.Register(new(RedisSubscriber3))
	if err != nil {
		t.Errorf("注册观察者出错,err:%s", err.Error())
		return
	}
	err = eventBus.Publish(context.Background(), Event{
		Topic: RedisTopic,
		Data:  RedisData1{},
	}, WithLeastDelayOption(time.Second*5))
	if err != nil {
		t.Errorf("推送失败,err:%s", err.Error())
		return
	}
	t.Logf("推送成功:%v", time.Now())
	time.Sleep(time.Second * 10)
}

type RedisSubscriber3 struct {
}

func (RedisSubscriber3) Event() Event {
	return Event{
		Topic: RedisTopic,
		Data:  RedisData1{},
	}
}
func (sub RedisSubscriber3) Handle(ctx context.Context, data interface{}) error {
	fmt.Println("收到消息:", time.Now())
	return nil
}

func TestRedisEventBus_GetTopicInfo(t *testing.T) {
	client, err := GetRedisClient()
	if err != nil {
		t.Errorf("创建Redis客户端出错,err:%s", err.Error())
		return
	}
	eventBus := NewRedisEventBus(client)
	err = eventBus.Register(new(RedisSubscriber3))
	if err != nil {
		t.Errorf("注册观察者出错,err:%s", err.Error())
		return
	}
	n := rand.Intn(10) + 5
	for i := 0; i < n; i++ {
		err = eventBus.Publish(context.Background(), Event{
			Topic: RedisTopic,
			Data:  RedisData1{},
		}, WithLeastDelayOption(time.Second*5))
		if err != nil {
			t.Errorf("推送失败,err:%s", err.Error())
			return
		}
	}
	info, err := eventBus.GetTopicInfo()
	if err != nil {
		t.Errorf("获取主题信息出错,err:%s", err.Error())
		return
	}
	m := TopicInfoListToMap(info)
	topicInfo, ok := m[RedisTopic]
	if !ok {
		t.Errorf("获取topic信息失败")
		return
	}
	if int(topicInfo.MessageCount) != n {
		t.Errorf("消息数量与预期不符")
		return
	} else {
		t.Logf("消息数量与预期符合")
	}
	time.Sleep(time.Second * 10)
}

func TopicInfoListToMap(list []TopicInfo) map[Topic]TopicInfo {
	result := make(map[Topic]TopicInfo)
	for _, item := range list {
		result[item.Topic] = item
	}
	return result
}
