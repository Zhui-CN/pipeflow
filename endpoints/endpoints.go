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

// Forks dynamic forks output
type Forks []tasks.Hop

// DynamicOutput dynamic forks output
type DynamicOutput struct {
	Forks Forks
}

// GetNextTasks get hop next tasks
func (d *DynamicOutput) GetNextTasks(output *Output) []*tasks.MetaTask {
	task := output.Task.(*tasks.MetaTask)
	if len(d.Forks) > 0 {
		task.AddHops(d.Forks)
	}
	return task.GetNextTasks()
}
