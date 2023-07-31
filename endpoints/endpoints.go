package endpoints

import "github.com/Zhui-CN/pipeflow/tasks"

type InputEndpoint interface {
	Get() tasks.Task
}
