package tasks

import (
	"encoding/json"
	"log"
)

type Hop struct {
	Params any    `json:"params"` // current hop params
	Queue  string `json:"queue"`  // queue name
	Next   []Hop  `json:"next"`   // next hop info
}

type meta struct {
	Params any `json:"params"` // meta params
	Hop    Hop `json:"hop"`    // hop info
}

type metaData struct {
	Meta meta `json:"meta"`
	Data any  `json:"data"`
}

/*
MetaTask
like task, additionally,
add metaData containing hops conf which determine
following dynamic outputEndpoints how to publish message
*/
type MetaTask struct {
	*task
	MetaData *metaData // metaData info
	NextHop  bool      // whether following
}

func (t *MetaTask) SetData(data any) {
	t.MetaData.Data = data
}

func (t *MetaTask) JsonUnmarshalData(v any) {
	byteData, _ := json.Marshal(t.MetaData.Data)
	if err := json.Unmarshal(byteData, v); err != nil {
		log.Panicln("json Unmarshal error:", err.Error())
	}
}

// GetNextTasks get hop next tasks
func (t *MetaTask) GetNextTasks() []*MetaTask {
	var nextTasks []*MetaTask
	if t.NextHop {
		for _, hop := range t.MetaData.Meta.Hop.Next {
			md := &metaData{
				Meta: meta{Params: t.MetaData.Meta.Params, Hop: hop},
				Data: t.MetaData.Data,
			}
			byteData, _ := json.Marshal(md)
			metaTask := &MetaTask{task: &task{data: byteData}, MetaData: md, NextHop: true}
			nextTasks = append(nextTasks, metaTask)
		}
	} else {
		nextTasks = append(nextTasks, t)
	}
	return nextTasks
}

// AddHops append hops
func (t *MetaTask) AddHops(hops []Hop) {
	t.MetaData.Meta.Hop.Next = append(t.MetaData.Meta.Hop.Next, hops...)
}

// Spawn spawn new task
func (t *MetaTask) Spawn(data any, nextHop bool, hopConf *Hop) *MetaTask {
	md := &metaData{
		Meta: t.MetaData.Meta,
		Data: data,
	}
	if hopConf != nil {
		md.Meta.Hop = *hopConf
	}
	byteData, _ := json.Marshal(md)
	return &MetaTask{
		task: &task{
			data:          byteData,
			toName:        t.toName,
			fromName:      t.fromName,
			confirmHandle: t.confirmHandle,
		},
		MetaData: md,
		NextHop:  nextHop,
	}
}

func NewMetaTask(data []byte) Task {
	var md metaData
	if err := json.Unmarshal(data, &md); err != nil {
		log.Panicln("NewMetaTask json Unmarshal error:", err.Error())
	}
	return &MetaTask{
		task:     &task{data: data},
		MetaData: &md,
		NextHop:  true,
	}
}
