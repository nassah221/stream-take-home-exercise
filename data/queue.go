package data

import (
	"exp/job-queue/pkg/job"
	"exp/job-queue/pkg/runner"
	"fmt"
	"log"
)

type Queue interface {
	Enqueue(job.Job)
	Dequeue() (*job.Job, error)
	Conclude(int) error
	GetJobByID(int) (*job.Job, error)
}

type queue struct {
	jobs     map[int]*job.Job
	enqueued chan job.Job
	runner.JobRunner
}

func NewQueue(bufSize int) Queue {
	q := queue{
		jobs:     make(map[int]*job.Job),
		enqueued: make(chan job.Job, bufSize),
	}
	q.JobRunner = runner.NewJobRunner(q.enqueued)
	q.JobRunner.Start(10)

	return &q
}

func (q *queue) Enqueue(j job.Job) {
	q.jobs[j.ID] = &j
	q.enqueued <- j
}

func (q *queue) Dequeue() (*job.Job, error) {
	var deqJob *job.Job
	done := make(chan struct{})
	go func() {
		deqJob = <-q.JobRunner.DequeuedJobChan()
		done <- struct{}{}
	}()
	q.JobRunner.DequeueSignal() <- struct{}{}
	<-done
	if deqJob != nil {
		q.jobs[deqJob.ID] = deqJob
		return deqJob, nil
	}

	return &job.Job{}, fmt.Errorf("no jobs queued or all workers busy")
}

func (q *queue) Conclude(id int) error {
	j, ok := q.jobs[id]
	if !ok {
		return fmt.Errorf("job %d not found", id)
	}

	if j.Status != job.InProgress {
		if j.Status == job.Queued {
			return fmt.Errorf("job is enqueued")
		}

		return fmt.Errorf("job has already concluded")
	}

	if workerID := j.Worker; workerID != 0 {
		log.Printf("Job %d is assigned to Worker %d", j.ID, j.Worker)

		done := make(chan struct{})
		go func() {
			_ = <-q.JobRunner.ConcludedJobChan()
			done <- struct{}{}
		}()
		q.JobRunner.ConcludeSignal() <- runner.ConcludeJobSignal{JobID: j.ID, WorkerID: j.Worker}
		<-done

		return nil
	}
	log.Println("[This should not be possible] Worker id should not be zero")
	return fmt.Errorf("unexpected error")
}

func (q *queue) GetJobByID(id int) (*job.Job, error) {
	job, ok := q.jobs[id]
	if !ok {
		return job, fmt.Errorf("job not found")
	}

	return job, nil
}
