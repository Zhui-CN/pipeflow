package main

import (
	"fmt"
	"github.com/Zhui-CN/pipeflow/server"
)

func worker1(params ...any) {
	fmt.Println("worker1 params:", params)
}

func worker2(params ...any) {
	fmt.Println("worker2 params:", params)
}

func main() {
	s := server.New()
	s.AddRoutineWorker(worker1, 3, true, "1", "2", "3")
	s.AddRoutineWorker(worker2, 5, false, "4", "5", "6")
	s.Run()
}
