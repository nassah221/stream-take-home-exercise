package main

// I took the basic idea about job workers & dispatcher from the following article
// http://nesv.github.io/golang/2014/02/25/worker-queues-in-go.html

// Implementing a job queue was a totally new concept for me and
// my first approach was simplistic because I hadn't processed what I had read
// on the internet

// The mentioned article seemed like a reasonable approach. I followed it and
// made changes to keep track of jobs when they're running/dequeued

import (
	"context"
	"exp/job-queue/data"
	"exp/job-queue/handlers"
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

func main() {
	flag.Parse()

	if *maxQueue < 1 {
		log.Fatal("Queue buffer cannot be negative")
	}

	l := log.New(os.Stdout, "job-queue-api ", log.LstdFlags)
	q := data.NewQueue(*maxQueue)
	h := handlers.New(l, q)
	// globalQueue = NewQueue(*maxQueue)
	// Start with 10 workers - again, I'm assuming that this is an ok number to start with for an exercise
	// Even though the queue has its own buffer, for actually running the jobs I'm assuming the number of workers
	// as another parameter to tune/change

	sm := mux.NewRouter()

	postRouter := sm.Methods(http.MethodPost).Subrouter()
	postRouter.HandleFunc("/jobs/enqueue", h.EnqueueHandler)
	postRouter.HandleFunc("/jobs/dequeue", h.DequeueHandler)
	postRouter.HandleFunc("/jobs/{id:[0-9]+}/conclude", h.ConcludeHandler)

	getRouter := sm.Methods(http.MethodGet).Subrouter()
	getRouter.HandleFunc("/jobs/{id:[0-9]+}", h.GetJobHandler)

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
