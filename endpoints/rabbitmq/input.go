package rmqEndpoint

import (
	"github.com/Zhui-CN/pipeflow/endpoints"
	"github.com/Zhui-CN/pipeflow/tasks"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

func confirm(m *amqp.Delivery) func() {
	return func() {
		m.Ack(false)
	}
}

type inputEndpoint struct {
	autoConfirm bool
	innerChan   <-chan amqp.Delivery
	funcType    tasks.TaskFuncType
}

func (p *inputEndpoint) Get() tasks.Task {
	msg := <-p.innerChan
	task := p.funcType(msg.Body)
	if p.autoConfirm {
		msg.Ack(false)
	} else {
		task.SetConfirmHandle(confirm(&msg))
	}
	return task
}

func NewInputEndpoint(conf Conf, queue string, qos int, autoConfirm bool, taskType tasks.TaskFuncType) endpoints.InputEndpoint {
	rmq := newRmq(conf)
	if queue == "" {
		log.Fatal("invalid queue:", queue)
	}
	if qos < 1 {
		log.Fatal("qos must be greater than 0")
	}
	if taskType == nil {
		taskType = tasks.RawType
	}
	rmq.channel.Qos(qos, 0, false)
	rmq.queueDeclare(queue)
	deliveryChan, err := rmq.channel.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		log.Fatal("cannot allocate consume:", err.Error())
	}
	endpoint := &inputEndpoint{
		autoConfirm: autoConfirm,
		innerChan:   deliveryChan,
		funcType:    taskType,
	}
	return endpoint
}
