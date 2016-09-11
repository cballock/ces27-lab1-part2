package mapreduce

import (
	"log"
    "sync"
)

// Schedules map operations on remote workers. This will run until InputFilePathChan
// is closed. If there is no worker available, it'll block.
func (master *Master) schedule(task *Task, proc string, filePathChan chan string) int {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////
    
    var (
        wg sync.WaitGroup
    )

	log.Printf("Scheduling %v operations\n", proc)
    
    counter := 0
    for filePath := range filePathChan {
        wg.Add(1)
        go master.runOperation(<-master.idleWorkerChan, &Operation{proc, counter, filePath}, &wg)
        counter++
    }
    
    wg.Wait()
    
    log.Printf("%vx %v operations completed\n", counter, proc)
	return counter
}

// runOperation start a single operation on a RemoteWorker and wait for it to return or fail.
func (master *Master) runOperation(remoteWorker *RemoteWorker, operation *Operation, wg *sync.WaitGroup) {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////

	var (
		err  error
		args *RunArgs
	)

	log.Printf("Running %v (ID: '%v' File: '%v' Worker: '%v')\n", operation.proc, operation.id, operation.filePath, remoteWorker.id)

	args = &RunArgs{operation.id, operation.filePath}
    err = remoteWorker.callRemoteWorker(operation.proc, args, new(struct{}))

	if err != nil {
		log.Printf("Operation %v '%v' Failed. Error: %v\n", operation.proc, operation.id, err)
        master.failedWorkerChan <- remoteWorker
        go master.runOperation(<-master.idleWorkerChan, operation, wg) // Re-schedule current operation for further reprocessing;
	} else {
        master.idleWorkerChan <- remoteWorker
        wg.Done()
	}
}
