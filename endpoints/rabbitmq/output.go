package rmqEndpoint

import (
	"context"
	"errors"
	"fmt"
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

// NewOutputEndpoint create rmq output endpoint controller
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

// use meta data in task to determine topics to pub, and update meta data.
type dynamicOutputEndpoint struct {
	*outputEndpoint
	*endpoints.DynamicOutput
}

func (p *dynamicOutputEndpoint) Put(output *endpoints.Output) {
	p.outputEndpoint.Put(output)
	nextTasks := p.GetNextTasks(output)
	for idx := range nextTasks {
		hop := nextTasks[idx].MetaData.Meta.Hop
		if hop.Queue == "" {
			fmt.Println(errors.New("invalid queue:" + hop.Queue).Error())
			continue
		}
		nextOutput := &endpoints.Output{
			Task:   nextTasks[idx],
			Params: OutputParams{Queue: hop.Queue},
		}
		p.outputEndpoint.Put(nextOutput)
	}
}

// NewDynamicOutputEndpoint create rmq dynamic output endpoint controller
func NewDynamicOutputEndpoint(conf Conf, forks endpoints.Forks, concurrency int, bufferSize int) endpoints.OutputEndpoint {
	return &dynamicOutputEndpoint{
		outputEndpoint: NewOutputEndpoint(conf, concurrency, bufferSize).(*outputEndpoint),
		DynamicOutput:  &endpoints.DynamicOutput{Forks: forks},
	}
}
