package tasks

import (
	"encoding/json"
	"log"
)

type RawTask struct {
	*task
}

func (t *RawTask) SetData(data any) {
	t.data = data.([]byte)
}

func (t *RawTask) JsonUnmarshalData(v any) {
	if err := json.Unmarshal(t.data, v); err != nil {
		log.Panicln("json Unmarshal error:", err.Error())
	}
}

func NewRawTask(data []byte) Task {
	return &RawTask{task: &task{data: data}}
}
