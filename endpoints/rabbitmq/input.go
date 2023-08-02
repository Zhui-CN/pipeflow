package rmqEndpoint

import (
	"github.com/Zhui-CN/pipeflow/endpoints"
	"github.com/Zhui-CN/pipeflow/tasks"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

// rmq confirm handle
func confirm(m *amqp.Delivery) func() {
	return func() {
		m.Ack(false)
	}
}

// rmq input endpoint controller
type inputEndpoint struct {
	autoConfirm  bool
	innerChan    chan *amqp.Delivery
	taskTypeFunc tasks.TaskTypeFunc
	deliveryChan <-chan amqp.Delivery
}

// rmq handle put rmq message in chan
func (p *inputEndpoint) handleMessage() {
	for {
		msg := <-p.deliveryChan
		p.innerChan <- &msg
	}
}

// Get rmq message from chan
func (p *inputEndpoint) Get() tasks.Task {
	msg := <-p.innerChan
	task := p.taskTypeFunc(msg.Body)
	if p.autoConfirm {
		msg.Ack(false)
	} else {
		task.SetConfirmHandle(confirm(msg))
	}
	return task
}

/*
NewInputEndpoint
Create RawType or MetaType rmq input endpoint controller
taskType: task type of RawType or MetaType
autoConfirm: whether to confirm automatically
*/
func NewInputEndpoint(conf Conf, queue string, qos int, autoConfirm bool, taskType tasks.TaskTypeFunc) endpoints.InputEndpoint {
	rmq := newRmq(conf)
	if queue == "" {
		log.Fatalln("invalid queue:", queue)
	}
	if qos < 1 {
		log.Fatalln("qos must be greater than 0")
	}
	if taskType == nil {
		taskType = tasks.RawType
	}
	rmq.channel.Qos(qos, 0, false)
	rmq.queueDeclare(queue)
	deliveryChan, err := rmq.channel.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalln("cannot allocate consume:", err.Error())
	}
	endpoint := &inputEndpoint{
		autoConfirm:  autoConfirm,
		innerChan:    make(chan *amqp.Delivery, qos),
		taskTypeFunc: taskType,
		deliveryChan: deliveryChan,
	}
	go endpoint.handleMessage()
	return endpoint
}
