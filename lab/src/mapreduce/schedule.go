package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os) (%d files)\n", ntasks, phase, n_other, len(mapFiles))

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var wg sync.WaitGroup


	var task_mgr = TaskMap{m: make(map[int]int)}
	for i:=0; i< ntasks; i ++{
		task_mgr.m[i] = taskInit
	}

	for {
		all_complete, _ := IsAllState(task_mgr, taskComplete)
		if all_complete{
			fmt.Printf("Schedule: %v TaskComplete\n", phase)
			break
		}

		for {
			all_assagn, task_index := IsAllState(task_mgr, taskAsssign)
			if all_assagn{
				fmt.Printf("Schedule: %v AssignComplete\n", phase)
				wg.Wait()
				break
			}
			worker := <- registerChan

			task_mgr.Lock()
			task_mgr.m[task_index] = taskAsssign
			task_mgr.Unlock()
			wg.Add(1)
			go func(worker string, i int){
				fmt.Printf("worker %s AssignTask: %d \n", worker, i)
				args := &DoTaskArgs{JobName:jobName, File:mapFiles[i], Phase:phase, TaskNumber:i, NumOtherPhase:n_other}
				ok := call(worker, "Worker.DoTask", args, new(struct{}))

				task_state := taskComplete
				if !ok {
					task_state = taskInit
				}
				task_mgr.Lock()
				task_mgr.m[task_index] = task_state
				task_mgr.Unlock()

				// 这里的chan 是无缓冲的chan，必须注意先发送完成，然后添加worker
				// 反之，因为chan无缓冲，只添加不读取导致阻塞，不发送完成，程序一直等待！
				wg.Done()
				if ok{
					registerChan <- worker
				}
				fmt.Printf("worker %s AssignTask: %d Result %d\n", worker, i, ok)
			}(worker, task_index)
		}
	}
}

func IsAllState(m TaskMap, state int) (bool, int){
	m.RLock()
	defer m.RUnlock()
	for task_index := range m.m{
		if m.m[task_index] < state{
			return false, task_index
		}
	}
	return true, 0
}



