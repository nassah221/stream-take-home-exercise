package main

import "log"

type Worker struct {
	id          int
	job         chan *Job
	workerQueue chan chan *Job
	stop        chan struct{}
	conclude    chan struct{}
	currentJob  *Job
}

func NewWorker(id int, workerQueue chan chan *Job) Worker {
	return Worker{
		id:          id,
		job:         make(chan *Job),
		workerQueue: workerQueue,
		stop:        make(chan struct{}),
		conclude:    make(chan struct{}),
	}
}

func (w *Worker) Start() {
	go func() {
	loop:
		for {
			w.workerQueue <- w.job
			log.Printf("Worker %d is available", w.id)

			select {
			case j := <-w.job:
				j.Worker = w.id
				w.currentJob = j
				log.Printf("Worker %d got job %d", w.id, j.ID)

				j.Status = InProgress
				log.Printf("Job %d in progress", j.ID)
				jobRunner.dequeuedJob <- j
				// Hold on to the job until concluded - usually there will be a job processor
				// that the worker is going to block on until it gets the conclude signal
				<-w.conclude
				log.Printf("Worker %d is concluding job %d", w.id, j.ID)
				j.Status = Concluded

				// nil value for worker
				j.Worker = 0
				w.currentJob = nil

				jobRunner.concludedJob <- j
				break

			case <-w.stop:
				break loop
			}
		}
	}()
}
