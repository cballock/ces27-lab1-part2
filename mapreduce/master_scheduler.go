package mapreduce

import (
	"log"
)

// Schedules map operations on remote workers. This will run until InputFilePathChan
// is closed. If there is no worker available, it'll block.
func (master *Master) schedule(task *Task, proc string, filePathChan chan string) int {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////
    
    var (
        filePath string
    )

	log.Printf("Scheduling %v operations\n", proc)
    
    master.failedOperationsChan = make(chan *Operation)
    master.hasNewFiles = true
    
    counter := 0
    for master.hasNewFiles || master.pendingOperationsCounter>0 {
        select {
            case filePath, master.hasNewFiles = <- filePathChan: // If filePathChan is open, master.hasNewFiles==true;
            if !master.hasNewFiles {
                filePathChan = nil // "Inactivate" filePathChan in the channel selector; this will cause filePathChan to be ignored in the select loop;
            } else {
                master.addToPendingOperationsCounter(1) // Increment master.pendingOperationsCounter;
                go master.runOperation(<-master.idleWorkerChan, &Operation{proc, counter, filePath})
                counter++
            }
            case operation, failedOperationsChannelOpen := <- master.failedOperationsChan:
            if failedOperationsChannelOpen {
                go master.runOperation(<-master.idleWorkerChan, operation)
            }
        }
    }
    
    log.Printf("%vx %v operations completed\n", counter, proc)
	return counter
}

// runOperation start a single operation on a RemoteWorker and wait for it to return or fail.
func (master *Master) runOperation(remoteWorker *RemoteWorker, operation *Operation) {
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
        master.failedOperationsChan <- operation // (*) Re-schedule current operation for further reprocessing;
		master.failedWorkerChan <- remoteWorker
	} else {
        master.addToPendingOperationsCounter(-1) // Decrement master.pendingOperationsCounter;
        master.idleWorkerChan <- remoteWorker
	}
}

func (master *Master) addToPendingOperationsCounter(amount int) {
    master.pendingOperationsCounterMutex.Lock()
    master.pendingOperationsCounter = master.pendingOperationsCounter + amount
    master.checkFailedOperationsChannelClosing()
    master.pendingOperationsCounterMutex.Unlock()
}

func (master *Master) checkFailedOperationsChannelClosing() {
    if !master.hasNewFiles && master.pendingOperationsCounter==0 { // No more failed operations to run since filePathChan doesn't have new files;
        close(master.failedOperationsChan) // This must be executed inside de mutex block to avoid simultaneous close calls on master.failedOperationsChan;
    }
}
