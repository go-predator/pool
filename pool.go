/*
 * @Author:    thepoy
 * @Email:     thepoy@163.com
 * @File Name: pool.go
 * @Created:   2022-05-23 15:31:38
 * @Modified:  2022-05-24 09:13:05
 */

package pool

import (
	"errors"
	"fmt"
	"reflect"
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
	// thrown when `Handle` is not a function type
	ErrNotFunc = errors.New("`Handle` only accepts function types")
)

var (
	errorType = reflect.TypeOf((*error)(nil)).Elem()
)

type Status uint8

// running status
const (
	STOPED Status = iota
	RUNNING
)

// Task task to-do
type Task struct {
	handle reflect.Value
	args   []reflect.Value
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

// NewTask creates a new pool task
func NewTask(handle interface{}, args ...interface{}) Task {
	f := reflect.ValueOf(handle)
	if f.Kind() != reflect.Func {
		panic("the handle is not a function")
	}
	if !goodFunc(f.Type()) {
		panic(fmt.Errorf("can't install function with %d results", f.Type().NumOut()))
	}

	v := make([]reflect.Value, 0, len(args))
	for _, arg := range args {
		v = append(v, reflect.ValueOf(arg))
	}

	return Task{
		handle: f,
		args:   v,
	}
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

// goodFunc reports whether the function or method has the right result signature.
func goodFunc(typ reflect.Type) bool {
	// We allow functions with 1 result or 2 results where the second is an error.
	switch {
	case typ.NumOut() == 1:
		return true
	case typ.NumOut() == 2 && typ.Out(1) == errorType:
		return true
	}
	return false
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
			task.handle.Call(task.args)
			// r := task.handle.Call(task.args)

			// returns := make([]interface{}, 0, len(r))
			// for _, i := range r {
			// 	returns = append(returns, i.Interface())
			// }
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
