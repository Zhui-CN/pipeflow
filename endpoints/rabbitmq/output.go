package rmqEndpoint

import (
	"context"
	"github.com/Zhui-CN/pipeflow/endpoints"
	amqp "github.com/rabbitmq/amqp091-go"
)

// OutputParams rmqOutputEndpoint result params
type OutputParams struct {
	Queue string
}

// rmq output endpoint controller
type outputEndpoint struct {
	*rmqClient
	pubChan             chan *endpoints.Output
	declareQueueNameMap map[string]struct{}
}

// pub handle
func (p *outputEndpoint) pubHandle() {
	go p.publish()
}

// publish message
func (p *outputEndpoint) publish() {
	var ok bool
	var err error
	for {
		output := <-p.pubChan
		queue := output.Params.(OutputParams).Queue
		if _, ok = p.declareQueueNameMap[queue]; !ok {
			p.queueDeclare(queue)
			p.declareQueueNameMap[queue] = struct{}{}
		}
		if err = p.channel.PublishWithContext(context.Background(), "", queue, false, false, amqp.Publishing{Body: output.Task.GetRawData()}); err == nil {
			output.Task.Confirm()
		}
	}
}

// Put task to rmq message
func (p *outputEndpoint) Put(output *endpoints.Output) {
	if output.Params != nil {
		p.pubChan <- output
	}
}

func NewOutputEndpoint(conf Conf, concurrency int, bufferSize int) endpoints.OutputEndpoint {
	if concurrency < 1 {
		concurrency = 1
	}
	if bufferSize < 1 {
		bufferSize = 30
	}
	endpoint := &outputEndpoint{
		rmqClient:           newRmq(conf),
		pubChan:             make(chan *endpoints.Output, bufferSize),
		declareQueueNameMap: make(map[string]struct{}),
	}
	for i := 0; i < concurrency; i++ {
		endpoint.pubHandle()
	}
	return endpoint
}
