package tasks

type TaskTypeFunc = func([]byte) Task

// Create task
var (
	RawType  = NewRawTask
	MetaType = NewMetaTask
)

type Task interface {
	Confirm()                 // confirm message
	SetData(any)              // set data
	GetRawData() []byte       // get raw data
	SetTo(string)             // set result task name of outputEndpoint
	GetTo() string            // get task of outputEndpoint name
	SetFrom(string)           // set input task name of inputEndpoint
	GetFrom() string          // get task of inputEndpoint name
	SetConfirmHandle(func())  // set task msg confirm handle func
	GetConfirmHandle() func() // get task msg confirm handle func
	JsonUnmarshalData(any)    // when data is json, this method can be used to quickly parse it into a struct
}

type task struct {
	data          []byte // raw data
	toName        string // control which output endpoint task will go
	fromName      string // indicate which input endpoint task come from,
	confirmHandle func() // func to confirm
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

func (t *task) GetRawData() []byte {
	return t.data
}

func (t *task) SetConfirmHandle(f func()) {
	t.confirmHandle = f
}

func (t *task) GetConfirmHandle() func() {
	return t.confirmHandle
}

func (t *task) Confirm() {
	if t.confirmHandle != nil {
		t.confirmHandle()
	}
}
