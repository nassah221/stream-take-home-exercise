package main

import "log"

type ConcludeJobSignal struct {
	WorkerID int
	JobID    int
}

var WorkerQueue chan chan *Job
var jobRunner = NewJobRunner()

type JobRunner struct {
	workers      map[int]*Worker        // Map of workers
	dequeueSig   chan struct{}          // Signal for the dispatcher to dequeue a job, comes from the handler
	concludeSig  chan ConcludeJobSignal // Signal for the dispatcher to conclude a job, comes from the handler
	dequeuedJob  chan *Job              // Dequeued job to return to the handler which it can send as a response
	concludedJob chan *Job
}

func NewJobRunner() JobRunner {
	return JobRunner{
		workers:      make(map[int]*Worker),
		dequeueSig:   make(chan struct{}),
		concludeSig:  make(chan ConcludeJobSignal),
		dequeuedJob:  make(chan *Job),
		concludedJob: make(chan *Job),
	}
}

func (d *JobRunner) StartJobRunner(nworkers int) {
	WorkerQueue = make(chan chan *Job, nworkers)

	// Create n number of workers
	for i := 1; i <= nworkers; i++ {
		log.Println("Starting worker", i)
		worker := NewWorker(i, WorkerQueue)

		// Insert the worker into the worker map and start each worker
		d.workers[i] = &worker
		worker.Start()
	}

	go func() {
		for {
			select {
			case <-d.dequeueSig:
				select {
				case worker := <-WorkerQueue:
					select {
					case j := <-globalQueue.Enqueued:
						worker <- &j
					default:
						log.Println("No jobs queued")
						jobRunner.dequeuedJob <- nil
					}
				default:
					log.Println("All workers busy")
					jobRunner.dequeuedJob <- nil
				}
			case s := <-d.concludeSig:
				worker, ok := d.workers[s.WorkerID]
				if !ok {
					log.Panicf("Worker %d not found", s.WorkerID)
				}
				log.Printf("Received conclude signal for Job %d assigned to Worker %d", s.JobID, s.WorkerID)
				worker.conclude <- struct{}{}
			}
		}
	}()
}
