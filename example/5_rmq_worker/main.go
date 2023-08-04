package main

import (
	"encoding/json"
	"fmt"
	rmqEndpoint "github.com/Zhui-CN/pipeflow/endpoints/rabbitmq"
	"github.com/Zhui-CN/pipeflow/server"
	"github.com/Zhui-CN/pipeflow/tasks"
)

func handle(group *server.Group, task tasks.Task) tasks.Task {
	fmt.Printf("group:%s from:%s data:%s\n", group.Name, task.GetFrom(), string(task.GetRawData()))
	newData1, _ := json.Marshal(map[string]string{"test": "ok"})
	task.SetData(newData1)
	task.SetTo("output")
	return task
}

func main() {
	s := server.New()
	rmqConf := rmqEndpoint.DefaultConf()

	group := s.AddGroup("myGroup", handle, 1)

	input := rmqEndpoint.NewInputEndpoint(rmqConf, "rmq_input_test", 1, true, tasks.RawType)
	output := rmqEndpoint.NewOutputEndpoint(rmqConf, 1, 5)

	group.AddInputEndpoint("input", input)
	group.AddOutputEndpoint("output", output, rmqEndpoint.OutputParams{Queue: "rmq_output_test"})

	s.Run()
}
