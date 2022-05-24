/*
 * @Author:    thepoy
 * @Email:     thepoy@163.com
 * @File Name: pool.go
 * @Created:   2022-05-23 15:31:38
 * @Modified:  2022-05-24 08:23:34
 */

package pool

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-predator/log"
)

// errors
var (
	// return if pool size <= 0
	ErrInvalidPoolCap = errors.New("invalid pool cap")
	// put task but pool already closed
	ErrPoolAlreadyClosed = errors.New("pool already closed")
	// only the error type can be captured and processed
	ErrUnkownType = errors.New("recover only allows error type, but an unknown type is received")
)

type Status uint8

// running status
const (
	STOPED Status = iota
	RUNNING
)

// Task task to-do
type Task struct {
	Handle func(args ...interface{}) interface{}
	Args   []interface{}
}

// Pool task pool
type Pool struct {
	capacity       uint32
	runningWorkers uint32
	status         Status
	chTask         chan *Task
	log            *log.Logger
	blockPanic     bool
	sync.Mutex
}

// Capacity return the capacity of the `Pool`
func (p *Pool) Capacity() uint32 {
	return p.capacity
}

// SetLogger sets `logger` for `Pool`
func (p *Pool) SetLogger(log *log.Logger) {
	p.log = log
}

// BlockPanic decides whether to panic when a goroutine throws an exception
func (p *Pool) BlockPanic(yes bool) {
	p.blockPanic = yes
}

// NewPool init pool
func NewPool(capacity uint32) (*Pool, error) {
	if capacity <= 0 {
		return nil, ErrInvalidPoolCap
	}
	p := &Pool{
		capacity: capacity,
		status:   RUNNING,
		chTask:   make(chan *Task, capacity),
	}

	return p, nil
}

func (p *Pool) checkWorker() {
	p.Lock()
	defer p.Unlock()

	if p.runningWorkers == 0 && len(p.chTask) > 0 {
		p.run()
	}
}

// GetRunningWorkers get running workers
func (p *Pool) GetRunningWorkers() uint32 {
	return atomic.LoadUint32(&p.runningWorkers)
}

func (p *Pool) incRunning() {
	atomic.AddUint32(&p.runningWorkers, 1)
}

func (p *Pool) decRunning() {
	atomic.AddUint32(&p.runningWorkers, ^uint32(0))
}

// Put put a task to pool
func (p *Pool) Put(task *Task) error {
	p.Lock()
	defer p.Unlock()

	if p.status == STOPED {
		return ErrPoolAlreadyClosed
	}

	// run worker
	if p.GetRunningWorkers() < p.Capacity() {
		p.run()
	}

	// send task
	if p.status == RUNNING {
		p.chTask <- task
	}

	return nil
}

func (p *Pool) run() {
	p.incRunning()

	go func() {
		defer func() {
			p.decRunning()

			if r := recover(); r != nil {
				if p.blockPanic {
					// 打印panic的堆栈信息
					debug.PrintStack()

					p.log.Error(fmt.Errorf("worker panic: %s", r))
				} else {
					// panic 只允许 error 类型
					if e, ok := r.(error); ok {
						panic(e)
					} else {
						panic(fmt.Sprintf("%s: %v", ErrUnkownType, r))
					}
				}
			}

			p.checkWorker() // check worker avoid no worker running
		}()

		for task := range p.chTask {
			task.Handle(task.Args...)
		}
	}()

}

func (p *Pool) setStatus(status Status) bool {
	p.Lock()
	defer p.Unlock()

	if p.status == status {
		return false
	}

	p.status = status

	return true
}

// Close close pool graceful
func (p *Pool) Close() {

	if !p.setStatus(STOPED) { // stop put task
		return
	}

	for len(p.chTask) > 0 { // wait all task be consumed
		time.Sleep(1e6) // reduce CPU load
	}

	close(p.chTask)
}
