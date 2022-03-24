package runner

import (
	"exp/job-queue/pkg/job"
	"log"
)

type ConcludeJobSignal struct {
	WorkerID int
	JobID    int
}

type JobRunner interface {
	Start(int)
	DequeuedJobChan() chan *job.Job
	DequeueSignal() chan struct{}
	ConcludedJobChan() chan *job.Job
	ConcludeSignal() chan ConcludeJobSignal
}

type jobRunner struct {
	workers      map[int]*Worker        // Map of workers
	dequeueSig   chan struct{}          // Signal for the dispatcher to dequeue a job, comes from the handler
	concludeSig  chan ConcludeJobSignal // Signal for the dispatcher to conclude a job, comes from the handler
	dequeuedJob  chan *job.Job          // Dequeued job to return to the handler which it can send as a response
	concludedJob chan *job.Job
	enqueued     chan job.Job
}

func NewJobRunner(enqChan chan job.Job) JobRunner {
	j := jobRunner{
		workers:      make(map[int]*Worker),
		dequeueSig:   make(chan struct{}),
		concludeSig:  make(chan ConcludeJobSignal),
		dequeuedJob:  make(chan *job.Job),
		concludedJob: make(chan *job.Job),
		enqueued:     enqChan,
	}
	// Set the global for communicating with the workers
	jr = j

	return &j
}

var WorkerQueue chan chan *job.Job
var jr jobRunner

func (j *jobRunner) DequeuedJobChan() chan *job.Job {
	return j.dequeuedJob
}

func (j *jobRunner) DequeueSignal() chan struct{} {
	return j.dequeueSig
}

func (j *jobRunner) ConcludedJobChan() chan *job.Job {
	return j.concludedJob
}

func (j *jobRunner) ConcludeSignal() chan ConcludeJobSignal {
	return j.concludeSig
}

func (j *jobRunner) Start(nworkers int) {
	WorkerQueue = make(chan chan *job.Job, nworkers)

	// Create n number of workers
	for i := 1; i <= nworkers; i++ {
		log.Println("Starting worker", i)
		worker := NewWorker(i, WorkerQueue)

		// Insert the worker into the worker map and start each worker
		j.workers[i] = &worker
		worker.Start()
	}

	go func() {
		for {
			select {
			case <-j.dequeueSig:
				select {
				case worker := <-WorkerQueue:
					select {
					case j := <-j.enqueued:
						worker <- &j
					default:
						log.Println("No jobs queued")
						j.dequeuedJob <- nil
					}
				default:
					log.Println("All workers busy")
					j.dequeuedJob <- nil
				}
			case s := <-j.concludeSig:
				worker, ok := j.workers[s.WorkerID]
				if !ok {
					log.Panicf("Worker %d not found", s.WorkerID)
				}
				log.Printf("Received conclude signal for Job %d assigned to Worker %d", s.JobID, s.WorkerID)
				worker.conclude <- struct{}{}
			}
		}
	}()
}
