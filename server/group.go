package server

import (
	"github.com/Zhui-CN/pipeflow/endpoints"
	"github.com/Zhui-CN/pipeflow/tasks"
	"log"
)

// group handle type
type handle func(*Group, tasks.Task) tasks.Task

// Group server
type Group struct {
	Name                  string                                              // group name
	concurrency           int                                                 // concurrency number
	handle                handle                                              // handle func
	taskChan              chan tasks.Task                                     // group inputEndpoint task chan
	endpointMap           map[string]struct{}                                 // store all endpoint
	outputParamsMap       map[string]any                                      // store group output task params
	outputEndpointChanMap map[endpoints.OutputEndpoint]chan *endpoints.Output // store outputEndpoint chan
	outputNameChanMap     map[string]chan *endpoints.Output                   // store output to name chan
}

func (g *Group) run() {
	for i := 0; i < g.concurrency; i++ {
		go func() {
			for {
				inputTask := <-g.taskChan
				outputTask := g.handle(g, inputTask)
				if outputTask != nil {
					g.Put(outputTask)
				}
			}
		}()
	}
}

// loop up get inputEndpoint task
func (g *Group) getTask(name string, inputEndpoint endpoints.InputEndpoint) {
	for {
		task := inputEndpoint.Get()
		task.SetFrom(name)
		g.taskChan <- task
	}
}

// loop up put outputEndpoint task
func (g *Group) putTask(resultChan chan *endpoints.Output, outputEndpoint endpoints.OutputEndpoint) {
	for {
		output := <-resultChan
		outputEndpoint.Put(output)
	}
}

// store all endpoint
func (g *Group) addEndpoint(name string) {
	if _, ok := g.endpointMap[name]; ok {
		log.Fatalf("endpoint %s already exist", name)
	}
	g.endpointMap[name] = struct{}{}
}

// Put push outputEndpoint task
func (g *Group) Put(task tasks.Task) {
	name := task.GetTo()
	output := &endpoints.Output{
		Task:   task,
		Params: g.outputParamsMap[name],
	}
	g.outputNameChanMap[name] <- output
}

// AddInputEndpoint add group inputEndpoint
func (g *Group) AddInputEndpoint(name string, inputEndpoint endpoints.InputEndpoint) {
	g.addEndpoint(name)
	go g.getTask(name, inputEndpoint)
}

/*
AddOutputEndpoint
add group outputEndpoint
outputParams: task params of endpoint type
*/
func (g *Group) AddOutputEndpoint(name string, outputEndpoint endpoints.OutputEndpoint, outputParams any) {
	g.addEndpoint(name)
	g.outputParamsMap[name] = outputParams
	if _, ok := g.outputEndpointChanMap[outputEndpoint]; !ok {
		resultChan := make(chan *endpoints.Output, g.concurrency)
		g.outputEndpointChanMap[outputEndpoint] = resultChan
		go g.putTask(resultChan, outputEndpoint)
	}
	g.outputNameChanMap[name] = g.outputEndpointChanMap[outputEndpoint]
}
