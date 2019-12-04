package backoff

import (
	"log"
	"math"
	"runtime/debug"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Backoff struct {
	conf    *Config
	started bool
}

// New Creates a new object for backoff.
func New(conf *Config) *Backoff {
	return &Backoff{
		conf: conf,
	}
}

// Start starts to consume data from SQS and enqueues the response to the
// queue using exponential backoff
func (b *Backoff) Start() {
	if b.started {
		return
	}

	b.started = true
	go b.processSQSMessage()
}

func (b *Backoff) processSQSMessage() {
	defer func() {
		if p := recover(); p != nil {
			log.Printf("Recovered from panic: %v\n %s", p, string(debug.Stack()))
			go b.processSQSMessage()
		}
	}()

	for {
		b.conf.rate <- struct{}{}
		go b.process()
	}
}

func (b *Backoff) process() {
	defer func() { <-b.conf.rate }()

	res, err := b.conf.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(10),
		QueueUrl:            aws.String(b.conf.sqsURL),
	})
	if err != nil {
		log.Printf("Unable to dequeue from SQS. Err: %+v", err)
		return
	}

	if len(res.Messages) == 0 {
		time.Sleep(1 * time.Second)
		return
	}

	for _, msg := range res.Messages {
		ok, response, retry := b.conf.process(*msg.Body)
		if !ok {
			continue
		}

		if response == "" {
			_, err := b.conf.sqs.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      aws.String(b.conf.sqsURL),
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				log.Printf("Unable to delete SQS message. Err: %+v", err)
				continue
			}

			continue
		}

		_, err := b.conf.sqs.SendMessage(&sqs.SendMessageInput{
			MessageBody:  aws.String(response),
			QueueUrl:     aws.String(b.conf.sqsURL),
			DelaySeconds: aws.Int64(int64(int(math.Pow(float64(b.conf.factor), float64(retry))))),
		})
		if err != nil {
			log.Printf("Unable to enqueue from SQS. Err: %+v", err)
			continue
		}
	}

}
