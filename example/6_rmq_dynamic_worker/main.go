package main

import (
	"fmt"
	"github.com/Zhui-CN/pipeflow/endpoints"
	rmqEndpoint "github.com/Zhui-CN/pipeflow/endpoints/rabbitmq"
	"github.com/Zhui-CN/pipeflow/server"
	"github.com/Zhui-CN/pipeflow/tasks"
)

/*
task message example
topic: rmq_input_dynamic_test
{
	"data": {
		"key1": "value1",
		"key2": "value2",
		"key3": "value3"
	},
	"meta": {
		"params": {
			"params1": "value1",
			"params2": "value2"
		},
		"hop": {
			"params": {
				"params1": "value1"
			},
			"queue": "input_dynamic_test",
			"next": [
				{
					"queue": "output_dynamic_test1",
					"next": [{"queue": "output_dynamic_test2"}]
				}
			]
		}
	}
}
*/

func handle(group *server.Group, task tasks.Task) tasks.Task {
	fmt.Printf("group name:%s from:%s data:%s\n", group.Name, task.GetFrom(), string(task.GetRawData()))
	newData1 := map[string]string{"test": "ok"}

	metaTask := task.(*tasks.MetaTask)

	task1 := metaTask.Spawn(newData1, true, nil)
	task1.SetTo("output1")
	group.Put(task1)

	task2 := metaTask.Spawn(newData1, true, nil)
	task2.SetTo("output2")
	group.Put(task2)

	task3 := metaTask.Spawn(newData1, true, nil)
	task3.SetTo("output3")
	group.Put(task3)

	return nil
}

func main() {
	s := server.New()

	rmqConf := rmqEndpoint.DefaultConf()

	group := s.AddGroup("myGroup", handle, 2)

	input := rmqEndpoint.NewInputEndpoint(rmqConf, "rmq_input_dynamic_test", 2, true, tasks.MetaType)
	group.AddInputEndpoint("input", input)

	output := rmqEndpoint.NewDynamicOutputEndpoint(rmqConf, nil, 1, 5)

	group.AddOutputEndpoint("output1", output, nil)
	group.AddOutputEndpoint("output2", output, rmqEndpoint.OutputParams{Queue: "rmq_output_params_test"})

	forks := endpoints.Forks{
		{Params: nil, Queue: "rmq_output_forks_test", Next: nil},
	}
	forkOutput := rmqEndpoint.NewDynamicOutputEndpoint(rmqConf, forks, 1, 5)
	group.AddOutputEndpoint("output3", forkOutput, nil)

	s.Run()
}
