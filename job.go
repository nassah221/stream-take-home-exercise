package main

import (
	"encoding/json"
	"io"
)

type Job struct {
	ID     int       `json:"ID,"`
	Type   JobType   `json:"Type,"`
	Status JobStatus `json:"Status,"`
}

type JobStatus string

const (
	Queued     JobStatus = "QUEUED"
	InProgress JobStatus = "IN_PROGRESS"
	Concluded  JobStatus = "CONCLUDED"
)

// I haven't thought about how the job type would come to affect the queue but here's what comes to my mind
// 1) Upon reaching a certain buffer capacity or after regular interavals, the job queue should be flushed into to temporary buffer
// 2) Time critical jobs should come first in order and the queue should be repopulated
// These are concerns for a job scheduler which would sit on top of the queue
type JobType string

const (
	Critical    JobType = "TIME_CRITICAL"
	NonCritical JobType = "NON_TIME_CRITICAL"
)

// Here I am violating the DRY principle and I would like to eliminate duplicatin
type EnqueueRequest struct {
	Type JobType `json:"Type"`
}

type EnqueueResponse struct {
	ID int `json:"ID"`
}

func (j *EnqueueRequest) FromJSON(r io.Reader) error {
	d := json.NewDecoder(r)
	return d.Decode(j)
}

func ToJSON(i interface{}, w io.Writer) error {
	e := json.NewEncoder(w)
	return e.Encode(i)
}

func (j *Job) ToJSON(w io.Writer) error {
	e := json.NewEncoder(w)
	return e.Encode(j)
}
