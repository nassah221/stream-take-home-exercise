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
