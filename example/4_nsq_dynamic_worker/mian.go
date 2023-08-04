package main

import (
	"fmt"
	"github.com/Zhui-CN/pipeflow/endpoints"
	nsqEndpoint "github.com/Zhui-CN/pipeflow/endpoints/nsq"
	"github.com/Zhui-CN/pipeflow/server"
	"github.com/Zhui-CN/pipeflow/tasks"
)

/*
task message example
topic: input_dynamic_test
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
                "params1": "value1",
				1: "a",
				2: 3,
				"asd": {"k": "v"},
            },
            "queue": "input_dynamic_test",
            "next": [
                {
                    "queue": "output_dynamic_test1"
                },
                {
                    "queue": "output_dynamic_test2",
                    "params": {
                        "delay": 0
                    }
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

	nsqConf := nsqEndpoint.DefaultConf()

	group := s.AddGroup("myGroup", handle, 2)

	input := nsqEndpoint.NewInputEndpoint(nsqConf, "input_dynamic_test", "test", 2, true, tasks.MetaType)
	group.AddInputEndpoint("input", input)

	output := nsqEndpoint.NewDynamicOutputEndpoint(nsqConf, nil, 1, 5)

	group.AddOutputEndpoint("output1", output, nil)
	group.AddOutputEndpoint("output2", output, nsqEndpoint.OutputParams{Topic: "output_params_test", Delay: 0})

	forks := endpoints.Forks{
		{Params: nil, Queue: "output_forks_test", Next: nil},
	}
	forkOutput := nsqEndpoint.NewDynamicOutputEndpoint(nsqConf, forks, 1, 5)
	group.AddOutputEndpoint("output3", forkOutput, nil)

	s.Run()

}
