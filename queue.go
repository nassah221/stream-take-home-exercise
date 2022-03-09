package main

import (
	"fmt"
	"log"
)

// I'm not sure if this is the appropriate way to keep track of job requests
// however, since every job_id is a unique int, I can look up from the map through it's key('ID')

// I'm also making an assumption that I have to keep track of all jobs even after their life-cycle ends
// i.e. after the job is concluded, so getting a concluded job will return a JSON response if the job existed in the first place

// There's also duplication in this implementation as I have to operate on the same job in 2 different places
// A single slice of all the jobs would be a cleaner solution but the trade-off would be verbosity
// 1) To search for a job to conclude, I would have to walk that single slice linearly until I find it. By separating the Dequeued jobs
// it's guaranteed that only the jobs that are dequeued are available to be concluded
// 2) Having a map allows me to look up a job_id with a single val, ok statement
type Queue struct {
	Jobs     map[int]*Job
	Enqueued chan Job
	Dequeued []*Job
}

func NewQueue(bufSize int) Queue {
	return Queue{
		Jobs:     make(map[int]*Job),
		Enqueued: make(chan Job, bufSize),
		Dequeued: make([]*Job, 0),
	}
}

func (q *Queue) Enqueue(j Job) {
	q.Jobs[j.ID] = &j
	q.Enqueued <- j
}

func (q *Queue) Dequeue() (*Job, error) {
	// Return an error if no are no jobs queued
	select {
	case j := <-q.Enqueued:
		// I'm not sure about this. Keeping track of the same object in multiple places is not ok
		// but I couldn't come up with a cleaner approach within the time limit
		trackedJob := q.Jobs[j.ID]
		trackedJob.Status = InProgress
		q.Dequeued = append(q.Dequeued, trackedJob)
		return trackedJob, nil
	default:
		return &Job{}, fmt.Errorf("no jobs queued")
	}
}

func (q *Queue) ConcludeJob(id int) error {
	for i, job := range q.Dequeued {
		if job.ID == id {
			trackedJob := q.Jobs[job.ID]
			trackedJob.Status = Concluded

			// If the job was found, remove it from the dequeued slice
			q.Dequeued = append(q.Dequeued[:i], q.Dequeued[i+1:]...)

			log.Printf("[DEBUG] Dequeued Jobs: %#+v", q.Dequeued)
			return nil
		}
	}
	return fmt.Errorf("job not found")
}

func (q *Queue) findJobByID(id int) (*Job, error) {
	job, ok := q.Jobs[id]
	if !ok {
		return job, fmt.Errorf("job not found")
	}

	return job, nil
}
