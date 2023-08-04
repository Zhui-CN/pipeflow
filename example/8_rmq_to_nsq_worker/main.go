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

	input := rmqEndpoint.NewInputEndpoint(rmqConf, "rmq_to_nsq_input", 1, true, tasks.RawType)
	output := nsqEndpoint.NewOutputEndpoint(nsqConf, 1, 50)

	group.AddInputEndpoint("input", input)
	group.AddOutputEndpoint("output", output, nsqEndpoint.OutputParams{Topic: "rmq_to_nsq_output", Delay: 0})

	s.Run()
}
