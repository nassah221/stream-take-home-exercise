package job

import (
	"encoding/json"
	"io"
)

type Status string

const (
	Queued     Status = "QUEUED"
	InProgress Status = "IN_PROGRESS"
	Concluded  Status = "CONCLUDED"
)

// I haven't thought about how the job type would come to affect the queue but here's what comes to my mind
// 1) Upon reaching a certain buffer capacity or after regular interavals, the job queue should be flushed into to temporary buffer
// 2) Time critical jobs should come first in order and the queue should be repopulated
// These are concerns for a job scheduler which would sit on top of the queue

type Type string

const (
	Critical    Type = "TIME_CRITICAL"
	NonCritical Type = "NON_TIME_CRITICAL"
)

type Job struct {
	ID     int    `json:"ID"`
	Type   Type   `json:"Type"`
	Status Status `json:"Status"`
	Worker int    `json:"-"`
}

type EnqueueRequest struct {
	Type Type `json:"Type"`
}

type EnqueueResponse struct {
	ID int `json:"ID"`
}

var jobCountID = 0

func NewJob(j EnqueueRequest) Job {
	job := Job{Type: j.Type, Status: Queued}
	jobCountID++
	job.ID = jobCountID

	return job
}

func (j *Job) ToJSON(w io.Writer) error {
	e := json.NewEncoder(w)
	return e.Encode(j)
}

func (j *EnqueueRequest) FromJSON(r io.Reader) error {
	d := json.NewDecoder(r)
	return d.Decode(j)
}
