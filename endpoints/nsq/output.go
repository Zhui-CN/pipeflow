package nsqEndpoint

import (
	"errors"
	"fmt"
	"github.com/Zhui-CN/pipeflow/endpoints"
	"github.com/nsqio/go-nsq"
	"log"
	"time"
)

// OutputParams nsqOutputEndpoint result params
type OutputParams struct {
	Topic string
	Delay time.Duration
}

// nsq output endpoint controller
type outputEndpoint struct {
	producer *nsq.Producer
	dPubChan chan *endpoints.Output
	mPubChan chan *endpoints.Output
}

// pub handle
func (p *outputEndpoint) pubHandle() {
	go p.deferredPublish()
	go p.multiPublish()
}

// deferredPublish message
func (p *outputEndpoint) deferredPublish() {
	var err error
	for {
		output := <-p.dPubChan
		params := output.Params.(OutputParams)
		if params.Delay < time.Second {
			params.Delay = time.Second * params.Delay
		}
		if err = p.producer.DeferredPublish(params.Topic, params.Delay, output.Task.GetRawData()); err == nil {
			output.Task.Confirm()
		}
	}
}

// multiPublish message
func (p *outputEndpoint) multiPublish() {
	var ok bool
	var err error
	var mPubMap map[string][][]byte
	var mConfirmMap map[string][]func()
	bufferSize := cap(p.mPubChan)
	for {
		select {
		case output := <-p.mPubChan:
			topic := output.Params.(OutputParams).Topic
			if mPubMap == nil {
				mPubMap = make(map[string][][]byte)
				mConfirmMap = make(map[string][]func())
			}
			if _, ok = mPubMap[topic]; !ok {
				mPubMap[topic] = make([][]byte, 0, bufferSize)
				mConfirmMap[topic] = make([]func(), 0, bufferSize)
			}
			mPubMap[topic] = append(mPubMap[topic], output.Task.GetRawData())
			mConfirmMap[topic] = append(mConfirmMap[topic], output.Task.Confirm)
		default:
			if mPubMap != nil {
				for topic := range mPubMap {
					if err = p.producer.MultiPublish(topic, mPubMap[topic]); err == nil {
						funcSlice := mConfirmMap[topic]
						for idx := range funcSlice {
							funcSlice[idx]()
						}
					}
				}
				mPubMap = nil
				mConfirmMap = nil
			}
		}
	}
}

// Put task to nsq message
func (p *outputEndpoint) Put(output *endpoints.Output) {
	if output.Params != nil {
		if output.Params.(OutputParams).Delay > 0 {
			p.dPubChan <- output
		} else {
			p.mPubChan <- output
		}
	}
}

// NewOutputEndpoint create nsq output endpoint controller
func NewOutputEndpoint(conf Conf, concurrency int, bufferSize int) endpoints.OutputEndpoint {
	if conf.NSQDTCPAddress == "" {
		log.Fatalln("nsq output must have NSQDTCPAddress")
	}
	cfg := nsq.NewConfig()
	cfg.DialTimeout = time.Second * 5
	cfg.WriteTimeout = time.Second * 30
	producer, err := nsq.NewProducer(conf.NSQDTCPAddress, cfg)
	if err != nil {
		log.Fatalln("connect nsqd tcp error:", err.Error())
	}
	if concurrency < 1 {
		concurrency = 1
	}
	if bufferSize < 1 {
		bufferSize = 30
	}
	endpoint := &outputEndpoint{
		producer: producer,
		dPubChan: make(chan *endpoints.Output, 2),
		mPubChan: make(chan *endpoints.Output, bufferSize),
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
	var delay float64
	p.outputEndpoint.Put(output)
	nextTasks := p.GetNextTasks(output)
	for idx := range nextTasks {
		hop := nextTasks[idx].MetaData.Meta.Hop
		if hop.Queue == "" || !nsq.IsValidTopicName(hop.Queue) {
			fmt.Println(errors.New("invalid topic:" + hop.Queue).Error())
			continue
		}
		hopParams := hop.Params
		if hopParams == nil || hopParams["delay"] == nil {
			delay = 0.0
		} else {
			delay = hopParams["delay"].(float64)
		}
		nextOutput := &endpoints.Output{
			Task: nextTasks[idx],
			Params: OutputParams{
				Topic: hop.Queue,
				Delay: time.Duration(delay),
			},
		}
		p.outputEndpoint.Put(nextOutput)
	}
}

// NewDynamicOutputEndpoint create nsq dynamic output endpoint controller
func NewDynamicOutputEndpoint(conf Conf, forks endpoints.Forks, concurrency int, bufferSize int) endpoints.OutputEndpoint {
	return &dynamicOutputEndpoint{
		outputEndpoint: NewOutputEndpoint(conf, concurrency, bufferSize).(*outputEndpoint),
		DynamicOutput:  &endpoints.DynamicOutput{Forks: forks},
	}
}
