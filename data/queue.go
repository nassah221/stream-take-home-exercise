package data

import (
	"exp/job-queue/pkg/job"
	"exp/job-queue/pkg/runner"
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

	// Start with 10 workers - again, I'm assuming that this is an ok number to start with for an exercise
	// Even though the queue has its own buffer, for actually running the jobs I'm assuming the number of workers
	// as another parameter to tune/change
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
