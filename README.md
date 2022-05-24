# pool

goroutine pool

## 注意

池中的任务需要考虑通用性，所以使用了反射(reflect)，也就会有一定的性能损失。

如果对性能要求较高，建议根据实际需求将任务函数和参数重写。

如：

```go
type handle func (a, b string) string

type Task struct {
    handle handle
    a string
    b string
}

func NewTask(f handle, a, b string) Task {
    return Task{
        handle: f,
        a: a,
        b: b
    }
}

...

for task := range p.chTask {
    task.handle(task.a, task.b)
}
```
