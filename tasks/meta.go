package tasks

import (
	"encoding/json"
	"log"
)

type Hop struct {
	Params any    `json:"params"`
	Queue  string `json:"queue"`
	Next   []Hop  `json:"next"`
}

type meta struct {
	Params any `json:"params"`
	Hop    Hop `json:"hop"`
}

type metaData struct {
	Meta meta
	Data any
}

type MetaTask struct {
	*task
	MetaData *metaData
	NextHop  bool
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

func (t *MetaTask) Spawn(data any, nextHop bool, hopConf *Hop) {

}

func newMetaTask(data []byte) Task {
	var md metaData
	if err := json.Unmarshal(data, &md); err != nil {
		log.Panicln("json Unmarshal error:", err.Error())
	}
	return &MetaTask{
		task:     &task{data: data},
		MetaData: &md,
		NextHop:  true,
	}
}
