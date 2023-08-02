package nsqEndpoint

import (
	"github.com/Zhui-CN/pipeflow/endpoints"
	"github.com/Zhui-CN/pipeflow/tasks"
	"github.com/nsqio/go-nsq"
	"log"
	"time"
)

// nsq confirm handle
func confirm(m *nsq.Message) func() {
	return func() {
		if !m.HasResponded() {
			m.Finish()
		}
	}
}

// nsq input endpoint controller
type inputEndpoint struct {
	autoConfirm  bool
	innerChan    chan *nsq.Message
	taskTypeFunc tasks.TaskTypeFunc
}

// HandleMessage nsq handle put nsq message in chan
func (p *inputEndpoint) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	p.innerChan <- message
	return nil
}

// Get nsq message from chan
func (p *inputEndpoint) Get() tasks.Task {
	msg := <-p.innerChan
	task := p.taskTypeFunc(msg.Body)
	if p.autoConfirm {
		msg.Finish()
	} else {
		task.SetConfirmHandle(confirm(msg))
	}
	return task
}

/*
NewInputEndpoint
Create RawType or MetaType nsq input endpoint controller
taskType: task type of RawType or MetaType
autoConfirm: whether to confirm automatically
*/
func NewInputEndpoint(conf Conf, topic string, channel string, maxInFlight int, autoConfirm bool, taskType tasks.TaskTypeFunc) endpoints.InputEndpoint {
	if len(conf.LookUpdHttpAddresses) < 1 && len(conf.NSQDTCPAddresses) < 1 {
		log.Fatalln("nsq inputEndpoint must have LookUpdHttpAddresses or NSQDTCPAddress")
	}
	if topic == "" || !nsq.IsValidTopicName(topic) {
		log.Fatalln("invalid topic:", topic)
	}
	if channel == "" || !nsq.IsValidChannelName(channel) {
		log.Fatalln("invalid Channel:", channel)
	}
	if maxInFlight < 1 {
		log.Fatalln("maxInFlight must be greater than 0")
	}
	if taskType == nil {
		taskType = tasks.RawType
	}
	endpoint := &inputEndpoint{
		autoConfirm:  autoConfirm,
		innerChan:    make(chan *nsq.Message, maxInFlight),
		taskTypeFunc: taskType,
	}
	cfg := nsq.NewConfig()
	cfg.DialTimeout = time.Second * 5
	cfg.MsgTimeout = time.Minute * 10
	cfg.LookupdPollInterval = time.Minute * 5
	cfg.MaxInFlight = maxInFlight
	consumer, err := nsq.NewConsumer(topic, channel, cfg)
	if err != nil {
		log.Fatalln("init consumer error:", err.Error())
	}
	consumer.AddHandler(endpoint)
	if len(conf.LookUpdHttpAddresses) != 0 {
		err = consumer.ConnectToNSQLookupds(conf.LookUpdHttpAddresses)
	} else {
		err = consumer.ConnectToNSQDs(conf.NSQDTCPAddresses)
	}
	if err != nil {
		log.Fatalln("nsq connect error:", err.Error())
	}
	return endpoint
}
