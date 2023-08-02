package endpoints

import "github.com/Zhui-CN/pipeflow/tasks"

// InputEndpoint get a message from queue and parse message into a task
type InputEndpoint interface {
	Get() tasks.Task
}

// OutputEndpoint parse task into message and put into queue
type OutputEndpoint interface {
	Put(*Output)
}

// Output parse handle result
type Output struct {
	Task   tasks.Task
	Params any
}
