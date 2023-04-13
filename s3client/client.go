package s3client

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func NewS3DataFetcher(cfg aws.Config, bucket string) func(context.Context, string) (io.ReadCloser, error) {
	client := s3.NewFromConfig(cfg)
	return func(ctx context.Context, key string) (io.ReadCloser, error) {
		res, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, err
		}
		return res.Body, nil
	}
}
