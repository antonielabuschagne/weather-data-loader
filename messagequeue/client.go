package messagequeue

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type MessageQueue struct {
	client   *sqs.Client
	queueUrl string
}

func NewMessageQueue(cfg aws.Config, queueUrl string) (mq MessageQueue) {
	mq.client = sqs.NewFromConfig(cfg)
	mq.queueUrl = queueUrl
	return
}

func (mq *MessageQueue) SendMessage(ctx context.Context, message string) (messageId string, err error) {
	msg := sqs.SendMessageInput{
		DelaySeconds: 10,
		MessageBody:  aws.String(message),
		QueueUrl:     aws.String(mq.queueUrl),
	}
	resp, err := mq.client.SendMessage(ctx, &msg)
	if err != nil {
		return
	}
	messageId = *resp.MessageId
	return
}
