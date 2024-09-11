package eventbus

import (
	"context"
	"testing"
)

const CtxMap = "ctx_map"

func TestLocalEventBus_Publish(t *testing.T) {
	eventBus := NewLocalEventBus()
	err := eventBus.Register(LocalSubscriber1{})
	if err != nil {
		t.Errorf("注册订阅者出现错误1,err:%s", err.Error())
		return
	}
	err = eventBus.Register(LocalSubscriber2{})
	if err != nil {
		t.Errorf("注册订阅者出现错误2,err:%s", err.Error())
		return
	}
	ctx := WithEventBus(nil)
	m := make(map[string]struct{})
	ctx = context.WithValue(ctx, CtxMap, m)
	err = eventBus.Publish(ctx, Event{
		Topic: LocalTopic1,
		Data:  LocalData1{},
	})
	if err != nil {
		t.Errorf("推送事件出错,err:%s", err.Error())
		return
	}
	err = eventBus.Publish(ctx, Event{
		Topic: LocalTopic2,
		Data:  LocalData2{},
	})
	if err != nil {
		t.Errorf("推送事件出错,err:%s", err.Error())
		return
	}
	_, ok := m[LocalTopic1]
	if ok {
		t.Logf("推送到LocalTopic1的观察者")
	} else {
		t.Errorf("推送到LocalTopic1的观察者")
	}
	_, ok = m[LocalTopic2]
	if ok {
		t.Logf("推送到LocalTopic2的观察者")
	} else {
		t.Errorf("推送到LocalTopic2的观察者")
	}
}

const LocalTopic1 = "local_topic1"
const LocalTopic2 = "local_topic2"

type LocalData1 struct {
}
type LocalSubscriber1 struct {
}

func (LocalSubscriber1) Event() Event {
	return Event{
		Topic: LocalTopic1,
		Data:  LocalData1{},
	}
}
func (LocalSubscriber1) Handle(ctx context.Context, data interface{}) error {
	_, ok := data.(LocalData1)
	if !ok {
		return nil
	}
	m := ctx.Value(CtxMap).(map[string]struct{})
	m[LocalTopic1] = struct{}{}
	return nil
}

type LocalData2 struct {
}
type LocalSubscriber2 struct {
}

func (LocalSubscriber2) Event() Event {
	return Event{
		Topic: LocalTopic2,
		Data:  LocalData2{},
	}
}
func (LocalSubscriber2) Handle(ctx context.Context, data interface{}) error {
	_, ok := data.(LocalData2)
	if !ok {
		return nil
	}
	m := ctx.Value(CtxMap).(map[string]struct{})
	m[LocalTopic2] = struct{}{}
	return nil
}
