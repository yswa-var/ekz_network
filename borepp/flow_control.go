package main

import "sync"

type FlowControl struct {
	mu     sync.Mutex
	cond   *sync.Cond
	Credit int
}

func NewFlowControl(credit int) *FlowControl {
	flow := &FlowControl{Credit: credit}
	flow.cond = sync.NewCond(&flow.mu)
	return flow
}

func (flow *FlowControl) WaitAndConsume(size int) {
	flow.mu.Lock()
	defer flow.mu.Unlock()

	for flow.Credit < size {
		flow.cond.Wait()
	}

	flow.Credit -= size
}

func (flow *FlowControl) Consume(size int) bool {
	flow.mu.Lock()
	defer flow.mu.Unlock()

	if size < 0 || flow.Credit < size {
		return false
	}

	flow.Credit -= size
	return true
}

func (flow *FlowControl) WindowUpdate(amount int) {
	if amount <= 0 {
		return
	}

	flow.mu.Lock()
	defer flow.mu.Unlock()

	flow.Credit += amount
	flow.cond.Broadcast()
}
