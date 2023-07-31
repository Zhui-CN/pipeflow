package nsqEndpoint

import (
	"github.com/Zhui-CN/pipeflow/endpoints"
	"github.com/Zhui-CN/pipeflow/tasks"
	"github.com/nsqio/go-nsq"
	"log"
	"time"
)

func confirm(m *nsq.Message) func() {
	return func() {
		if !m.HasResponded() {
			m.Finish()
		}
	}
}

type inputEndpoint struct {
	autoConfirm bool
	innerChan   chan *nsq.Message
	funcType    tasks.TaskFuncType
}

func (p *inputEndpoint) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	p.innerChan <- message
	return nil
}

func (p *inputEndpoint) Get() tasks.Task {
	msg := <-p.innerChan
	task := p.funcType(msg.Body)
	if p.autoConfirm {
		msg.Finish()
	} else {
		task.SetConfirmHandle(confirm(msg))
	}
	return task
}

func NewInputEndpoint(conf Conf, topic string, channel string, maxInFlight int, autoConfirm bool, taskType tasks.TaskFuncType) endpoints.InputEndpoint {
	if len(conf.LookUpdHttpAddresses) < 1 && len(conf.NSQDTCPAddresses) < 1 {
		log.Fatalf("nsq inputEndpoint must have LookUpdHttpAddresses or NSQDTCPAddress")
	}
	if topic == "" || !nsq.IsValidTopicName(topic) {
		log.Fatal("invalid topic:", topic)
	}
	if channel == "" || !nsq.IsValidChannelName(channel) {
		log.Fatal("invalid Channel:", channel)
	}
	if maxInFlight < 1 {
		log.Fatal("maxInFlight must be greater than 0")
	}
	if taskType == nil {
		taskType = tasks.RawType
	}
	endpoint := &inputEndpoint{
		autoConfirm: autoConfirm,
		innerChan:   make(chan *nsq.Message, maxInFlight),
		funcType:    taskType,
	}
	cfg := nsq.NewConfig()
	cfg.DialTimeout = time.Second * 5
	cfg.MsgTimeout = time.Minute * 10
	cfg.LookupdPollInterval = time.Minute * 5
	cfg.MaxInFlight = maxInFlight
	consumer, err := nsq.NewConsumer(topic, channel, cfg)
	if err != nil {
		log.Fatalf("init consumer error:%s", err.Error())
	}
	consumer.AddHandler(endpoint)
	if len(conf.LookUpdHttpAddresses) != 0 {
		err = consumer.ConnectToNSQLookupds(conf.LookUpdHttpAddresses)
	} else {
		err = consumer.ConnectToNSQDs(conf.NSQDTCPAddresses)
	}
	if err != nil {
		log.Fatalf("nsq connect error:%s", err.Error())
	}
	return endpoint
}
