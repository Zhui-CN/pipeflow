package tasks

type Task interface {
	Confirm()
	SetTo(string)
	GetTo() string
	SetFrom(string)
	GetFrom() string
	SetConfirmHandle(func())
	GetConfirmHandle() func()
	SetData(any)
	GetRawData() []byte
	JsonUnmarshalData(any)
}

var RawType = newRawTask
var MetaType = newMetaTask

type task struct {
	data          []byte
	toName        string
	fromName      string
	confirmHandle func()
}

func (t *task) SetTo(name string) {
	t.toName = name
}

func (t *task) GetTo() string {
	return t.toName
}

func (t *task) SetFrom(name string) {
	t.fromName = name
}

func (t *task) GetFrom() string {
	return t.fromName
}

func (t *task) SetConfirmHandle(f func()) {
	t.confirmHandle = f
}

func (t *task) GetConfirmHandle() func() {
	return t.confirmHandle
}

func (t *task) GetRawData() []byte {
	return t.data
}

func (t *task) Confirm() {
	if t.confirmHandle != nil {
		t.confirmHandle()
	}
}
