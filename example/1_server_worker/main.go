package main

import (
	"fmt"
	"github.com/Zhui-CN/pipeflow/server"
	"time"
)

func worker1(params ...any) {
	for {
		fmt.Println("worker1 params:", params)
		time.Sleep(time.Second * 2)
	}
}

func worker2(params ...any) {
	for {
		fmt.Println("worker2 params:", params)
		time.Sleep(time.Second * 6)
	}
}

func main() {
	s := server.New()
	s.AddWorker(worker1, "1", "2", 3)
	s.AddWorker(worker2, []any{}, map[string]string{"k": "v"})
	s.Run()
}
