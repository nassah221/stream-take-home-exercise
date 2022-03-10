package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/mux"
)

var bindAddr = flag.String("addr", ":9090", "Address to bind the server to")

// I'm assuming that 100 is a good size for a size bounded queue - not informed enough to make an educated decision
// as to how this parameter would affect the server

// I would also like to make an improvement to reject an enqueue job request while the internal queue buffer is full
var maxQueue = flag.Int("max_queue", 100, "Max size of bounded queue")

// I would like to use this type with all the error responses instead of just writing a string in http.Error
type GenericError struct {
	Message string `json:"message"`
}

// Global variables are frowned upon but I'm targeting max time limit of ~ 2 hrs
var globalQueue Queue

var jobCountID = 1

func NewJob(j EnqueueRequest) Job {
	job := Job{Type: j.Type, Status: Queued}
	jobCountID++
	job.ID = jobCountID

	return job
}

func main() {
	flag.Parse()

	if *maxQueue < 1 {
		log.Fatal("Queue buffer cannot be negative")
	}

	globalQueue = NewQueue(*maxQueue)

	sm := mux.NewRouter()

	postRouter := sm.Methods(http.MethodPost).Subrouter()
	postRouter.HandleFunc("/jobs/enqueue", EnqueueHandler)
	postRouter.HandleFunc("/jobs/dequeue", DequeueHandler)
	postRouter.HandleFunc("/jobs/{id:[0-9]+}/conclude", ConcludeHandler)

	getRouter := sm.Methods(http.MethodGet).Subrouter()
	getRouter.HandleFunc("/jobs/{id:[0-9]+}", GetJobHandler)

	s := http.Server{
		Addr:         *bindAddr,         // configure the bind address
		Handler:      sm,                // set the default handler
		ReadTimeout:  5 * time.Second,   // max time to read request from the client
		WriteTimeout: 10 * time.Second,  // max time to write response to the client
		IdleTimeout:  120 * time.Second, // max time for connections using TCP Keep-Alive
	}

	go func() {
		log.Println("Starting server on port 9090")

		err := s.ListenAndServe()
		if err != nil {
			log.Printf("Error starting server: %s\n", err)
			os.Exit(1)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// Block until a signal is received.
	sig := <-c
	log.Println("Got signal:", sig)

	// gracefully shutdown the server, waiting max 30 seconds for current operations to complete
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)

	if err := s.Shutdown(ctx); err != nil {
		log.Fatalf("[ERROR] shutting down server: %v", err)
	}
}
