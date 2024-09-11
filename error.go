package eventbus

import "errors"

var (
	ErrCtxNil               = errors.New("err ctx nil")
	ErrCtxNotFoundLoopTimes = errors.New("err ctx not found loop times")
	ErrCtxLoopTimesType     = errors.New("err ctx loop times type")
	ErrEventDataNil         = errors.New("err event data nil")
	ErrEventLoopOverflow    = errors.New("event loop overflow")
	ErrEventDataMustStruct  = errors.New("err event data must struct")
	ErrTopicEmpty           = errors.New("err topic empty")
	ErrSubscriberNil        = errors.New("err subscriber nil")
	ErrNotSupportOptions    = errors.New("err not support options")
)
