package backoff

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// ProcessFunc is callback function to process message from the SQS queue. Message body from the
// SQS queue is sent a parameter. It returns the following values
// ok (bool) - If the message has been process successfully. If yes the message is delete from queue
// and the returned message if available is sent to SQS with the given retry count in exponential manner
// If no then the message is not deleted from the queue.
// msg (string) - The message body that needs to be sent to SQS in exponential manner
// retry (int) - The number of retries base on which the exponential backoff is calculated
type ProcessFunc func(msg string) (bool, string, int)

// Config is Configuration for exponential backoff
type Config struct {
	process ProcessFunc
	sqsURL  string
	factor  int
	rate    chan struct{}
	sqs     *sqs.SQS
}

// NewConfig create a new Config object. It requires the following parameters
// sqsURL - URL of the SQS queue. Get it from the AWS console
// factor - Exponential retry factor. Eg. If factor is 3, retries are 3^0, 3^1, 3^2 etc.
// concurrency - Number of messages that need to be processed concurrently. ie. The number of goroutines to run at any time.
// process - Callback Function to process the message received from SQS queue
// Please ensure that aws config is updated to the environment variables as follows
// AWS_ACCESS_KEY_ID - access key
// AWS_SECRET_ACCESS_KEY - secret key
// AWS_REGION - AWS region
// If the application is running an ec2 instance, ensure that the instance has IAM role attached to it to access SQS.
func NewConfig(sqsURL string, factor, concurrency int, process ProcessFunc) *Config {
	return &Config{
		sqsURL:  sqsURL,
		factor:  factor,
		process: process,
		rate:    make(chan struct{}, concurrency),
		sqs:     sqs.New(session.New()),
	}
}
