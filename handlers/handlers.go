package handlers

import (
	"exp/job-queue/data"
	"exp/job-queue/pkg/job"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

type GenericError struct {
	Message string `json:"message"`
}

type Handler struct {
	l *log.Logger
	q data.Queue
}

func New(l *log.Logger, q data.Queue) Handler {
	return Handler{l, q}
}

func (h *Handler) EnqueueHandler(rw http.ResponseWriter, r *http.Request) {
	h.l.Println("[DEBUG] Handle Enqueue")

	var jobReq job.EnqueueRequest

	// I would like to implement better validation
	if err := jobReq.FromJSON(r.Body); err != nil {
		h.l.Printf("[ERROR] deserialzing product: %v", err)

		rw.WriteHeader(http.StatusBadRequest)
		ToJSON(&GenericError{Message: err.Error()}, rw)
		return
	}
	j := job.NewJob(jobReq)
	h.q.Enqueue(j)

	res := job.EnqueueResponse{ID: j.ID}

	rw.WriteHeader(http.StatusAccepted)
	ToJSON(&res, rw)
}

func (h *Handler) DequeueHandler(rw http.ResponseWriter, r *http.Request) {
	h.l.Println("Handle Dequeue")

	j, err := h.q.Dequeue()
	if err != nil {
		//todo use the generic error response
		http.Error(rw, "No jobs queued", http.StatusNotFound)
		return
	}

	rw.WriteHeader(http.StatusOK)
	j.ToJSON(rw)
}

func (h *Handler) ConcludeHandler(rw http.ResponseWriter, r *http.Request) {
	log.Println("Handle Conclude")

	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		//todo use the generic error response
		http.Error(rw, "Unable to parse id", http.StatusBadRequest)
		return
	}

	if err := h.q.Conclude(id); err != nil {
		h.l.Println(err)
		//todo use the generic error response
		// Based on the error, different http status codes should be used for all workers busy and job id not found
		http.Error(rw, err.Error(), http.StatusNotFound)
		return
	}

	rw.WriteHeader(http.StatusAccepted)
}

func (h *Handler) GetJobHandler(rw http.ResponseWriter, r *http.Request) {
	log.Println("Handle GetJob")

	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		//todo use the generic error response
		http.Error(rw, "Unable to parse id", http.StatusBadRequest)
		return
	}

	j, err := h.q.GetJobByID(id)
	if err != nil {
		//todo use the generic error response
		http.Error(rw, "Job id not found", http.StatusNotFound)
		return
	}

	rw.WriteHeader(http.StatusOK)
	j.ToJSON(rw)
}
