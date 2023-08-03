package server

import (
	"github.com/Zhui-CN/pipeflow/endpoints"
	"github.com/Zhui-CN/pipeflow/tasks"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Server struct {
	groupMap       map[string]*Group
	workers        []func()
	routineWorkers []func()
}

// AddWorker add server worker execute function
func (s Server) AddWorker(worker func(...any), params ...any) {
	s.workers = append(s.workers, func() { worker(params...) })
}

// AddRoutineWorker add server routine worker execute function
func (s Server) AddRoutineWorker(worker func(...any), second time.Duration, immediately bool, params ...any) {
	if second < time.Second {
		second = time.Second * second
	}
	s.routineWorkers = append(s.routineWorkers, func() {
		if immediately {
			worker(params...)
		}
		ticker := time.NewTicker(second)
		for {
			<-ticker.C
			worker(params...)
		}
	})
}

// AddGroup server group
func (s Server) AddGroup(name string, handle handle, concurrency int) *Group {
	if _, ok := s.groupMap[name]; ok {
		log.Fatalf("group %s already exist", name)
	}
	if concurrency < 1 {
		log.Fatalln("concurrency must be greater than 0")
	}
	if handle == nil {
		log.Fatalf("group %s not handle", name)
	}
	group := &Group{
		Name:                  name,
		concurrency:           concurrency,
		handle:                handle,
		taskChan:              make(chan tasks.Task, concurrency),
		endpointMap:           make(map[string]struct{}),
		outputParamsMap:       make(map[string]any),
		outputEndpointChanMap: make(map[endpoints.OutputEndpoint]chan *endpoints.Output),
		outputNameChanMap:     make(map[string]chan *endpoints.Output),
	}
	s.groupMap[name] = group
	return group
}

func (s Server) Run() {
	for _, f := range s.workers {
		go f()
	}
	for _, f := range s.routineWorkers {
		go f()
	}
	for _, group := range s.groupMap {
		group.run()
	}
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-quit
	log.Println("server finish")
}

func New() Server {
	return Server{groupMap: make(map[string]*Group)}
}
