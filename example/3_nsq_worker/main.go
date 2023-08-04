package main

import (
	"encoding/json"
	nsqEndpoint "github.com/Zhui-CN/pipeflow/endpoints/nsq"
	"github.com/Zhui-CN/pipeflow/server"
	"github.com/Zhui-CN/pipeflow/tasks"
	"log"
	"time"
)

func handle(group *server.Group, task tasks.Task) tasks.Task {
	log.Printf("group name:%s from:%s data:%s\n", group.Name, task.GetFrom(), string(task.GetRawData()))
	newData1, _ := json.Marshal(map[string]string{"test": "ok"})
	task.SetData(newData1)
	task.SetTo("output")
	time.Sleep(time.Second * 2)
	return task
}

func main() {
	s := server.New()

	nsqConf := nsqEndpoint.DefaultConf()

	group := s.AddGroup("myGroup", handle, 5)

	input := nsqEndpoint.NewInputEndpoint(nsqConf, "nsq_input_test", "test", 5, true, tasks.RawType)
	output := nsqEndpoint.NewOutputEndpoint(nsqConf, 1, 10)

	group.AddInputEndpoint("input", input)
	group.AddOutputEndpoint("output", output, nsqEndpoint.OutputParams{Topic: "nsq_output_test", Delay: 0})
	s.Run()
}
