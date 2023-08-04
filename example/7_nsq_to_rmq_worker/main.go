package main

import (
	"fmt"
	nsqEndpoint "github.com/Zhui-CN/pipeflow/endpoints/nsq"
	rmqEndpoint "github.com/Zhui-CN/pipeflow/endpoints/rabbitmq"
	"github.com/Zhui-CN/pipeflow/server"
	"github.com/Zhui-CN/pipeflow/tasks"
)

func handle(group *server.Group, task tasks.Task) tasks.Task {
	fmt.Printf("group name:%s from:%s data:%s\n", group.Name, task.GetFrom(), string(task.GetRawData()))
	task.SetTo("output")
	return task
}

func main() {
	s := server.New()

	nsqConf := nsqEndpoint.DefaultConf()
	rmqConf := rmqEndpoint.DefaultConf()

	group := s.AddGroup("myGroup", handle, 5)

	input := nsqEndpoint.NewInputEndpoint(nsqConf, "nsq_to_rmq_input", "test", 2, true, tasks.RawType)
	output := rmqEndpoint.NewOutputEndpoint(rmqConf, 1, 5)

	group.AddInputEndpoint("input", input)
	group.AddOutputEndpoint("output", output, rmqEndpoint.OutputParams{Queue: "nsq_to_rmq_output"})

	s.Run()
}
