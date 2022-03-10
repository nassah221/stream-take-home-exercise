package main

import (
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

func EnqueueHandler(rw http.ResponseWriter, r *http.Request) {
	log.Println("Handle Enqueue")

	var jobReq EnqueueRequest

	// I would like to implement better validation
	if err := jobReq.FromJSON(r.Body); err != nil {
		log.Printf("[ERROR] deserialzing product: %v", err)
		rw.WriteHeader(http.StatusBadRequest)

		ToJSON(&GenericError{Message: err.Error()}, rw)
		return
	}
	j := NewJob(jobReq)
	globalQueue.Enqueue(j)

	res := EnqueueResponse{ID: j.ID}
	ToJSON(&res, rw)
}

func DequeueHandler(rw http.ResponseWriter, r *http.Request) {
	log.Println("Handle Dequeue")
	job, err := globalQueue.Dequeue()

	if err != nil {
		http.Error(rw, "No jobs queued", http.StatusNotFound)
		return
	}

	job.ToJSON(rw)
}
func ConcludeHandler(rw http.ResponseWriter, r *http.Request) {
	log.Println("Handle Conclude")

	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		http.Error(rw, "Unable to parse id", http.StatusBadRequest)
		return
	}
	if err := globalQueue.ConcludeJob(id); err != nil {
		log.Println(err)
		// Based on the error, different http status codes should be used for all workers busy and job id not found
		http.Error(rw, err.Error(), http.StatusNotFound)
		return
	}

	rw.WriteHeader(http.StatusAccepted)
}

func GetJobHandler(rw http.ResponseWriter, r *http.Request) {
	log.Println("Handle GetJob")

	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		http.Error(rw, "Unable to parse id", http.StatusBadRequest)
		return
	}

	j, err := globalQueue.findJobByID(id)
	if err != nil {
		http.Error(rw, "Job id not found", http.StatusNotFound)
		return
	}

	j.ToJSON(rw)
}
