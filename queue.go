package main

import (
	"fmt"
	"log"
)

// I'm not sure if this is the appropriate way to keep track of job requests
// however, since every job_id is a unique int, I can look up from the map through its key('ID')

// I'm also making an assumption that I have to keep track of all jobs even after their life-cycle ends
// i.e. after the job is concluded, so getting a concluded job will return a JSON response if the job existed in the first place

// // There's also duplication in this implementation as I have to operate on the same job in 2 different places
// // A single slice of all the jobs would be a cleaner solution but the trade-off would be verbosity
// // 1) To search for a job to conclude, I would have to walk that single slice linearly until I find it. By separating the Dequeued jobs
// // it's guaranteed that only the jobs that are dequeued are available to be concluded
// // 2) Having a map allows me to look up a job_id with a single val, ok statement

// Dequeued data structure was just a place holder for the jobs that were running
// I have implemented a dispatcher and worker whose jobs are to dispatch jobs from the job queue
// and process said jobs respectively
type Queue struct {
	Jobs     map[int]*Job // A persistent collection of all jobs received
	Enqueued chan Job     // Job queue
}

func NewQueue(bufSize int) Queue {
	return Queue{
		Jobs:     make(map[int]*Job),
		Enqueued: make(chan Job, bufSize),
	}
}

func (q *Queue) Enqueue(j Job) {
	q.Jobs[j.ID] = &j
	q.Enqueued <- j
}

func (q *Queue) Dequeue() (*Job, error) {
	var deqJob *Job
	done := make(chan struct{})
	go func() {
		deqJob = <-jobRunner.dequeuedJob
		done <- struct{}{}
	}()
	jobRunner.dequeueSig <- struct{}{}
	<-done
	if deqJob != nil {
		q.Jobs[deqJob.ID] = deqJob
		return deqJob, nil
	}

	return &Job{}, fmt.Errorf("no jobs queued or all workers busy")
}

func (q *Queue) ConcludeJob(id int) error {
	j, ok := q.Jobs[id]
	if !ok {
		return fmt.Errorf("job %d not found", id)
	}

	if j.Status != InProgress {
		if j.Status == Queued {
			return fmt.Errorf("job is enqueued")
		}

		return fmt.Errorf("job has already concluded")
	}

	if workerID := j.Worker; workerID != 0 {
		log.Printf("Job %d is assigned to Worker %d", j.ID, j.Worker)

		done := make(chan struct{})
		go func() {
			_ = <-jobRunner.concludedJob
			done <- struct{}{}
		}()
		jobRunner.concludeSig <- ConcludeJobSignal{JobID: j.ID, WorkerID: j.Worker}
		<-done

		return nil
	}
	log.Println("[This should not be possible] Worker id should not be zero")
	return fmt.Errorf("unexpected error")
}

func (q *Queue) findJobByID(id int) (*Job, error) {
	job, ok := q.Jobs[id]
	if !ok {
		return job, fmt.Errorf("job not found")
	}

	return job, nil
}
