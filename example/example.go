package main

import (
	"os"
	"time"

	"github.com/FenixAra/exp-backoff/backoff"
)

var (
	sqsURL = os.Getenv("SQS_URL")
)

func main() {
	conf := backoff.NewConfig(sqsURL, 3, 1, func(msg string) (bool, string, int) {
		// Process the message received from SQS

		// Return, if the message should be sent to be retried with the retry count
		// and the message to be sent to the SQS
		return true, "send-data", 5
	})

	bo := backoff.New(conf)
	bo.Start()
	time.Sleep(1 * time.Hour)
}
