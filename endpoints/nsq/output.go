package nsqEndpoint

import (
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
	bufferSize int
	producer   *nsq.Producer
	dPubChan   chan *endpoints.Output
	mPubChan   chan *endpoints.Output
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
	for {
		select {
		case output := <-p.mPubChan:
			topic := output.Params.(OutputParams).Topic
			if mPubMap == nil {
				mPubMap = make(map[string][][]byte)
				mConfirmMap = make(map[string][]func())
			}
			if _, ok = mPubMap[topic]; !ok {
				mPubMap[topic] = make([][]byte, 0, p.bufferSize)
				mConfirmMap[topic] = make([]func(), 0, p.bufferSize)
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

func NewOutputEndpoint(conf Conf, concurrency int, bufferSize int) *outputEndpoint {
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
		bufferSize = 50
	}
	endpoint := &outputEndpoint{
		bufferSize: bufferSize,
		producer:   producer,
		dPubChan:   make(chan *endpoints.Output, 2),
		mPubChan:   make(chan *endpoints.Output, bufferSize),
	}
	for i := 0; i < concurrency; i++ {
		endpoint.pubHandle()
	}
	return endpoint
}
