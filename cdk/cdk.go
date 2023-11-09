package main

import (
	"os"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambda"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambdaeventsources"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3notifications"
	"github.com/aws/aws-cdk-go/awscdk/v2/awssqs"
	awslambdago "github.com/aws/aws-cdk-go/awscdklambdagoalpha/v2"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/joho/godotenv"
)

type CDKStackProps struct {
	WeatherAPIKey      *string
	WeatherAPIEndpoint *string
	StackProps         awscdk.StackProps
}

func NewCDKStack(scope constructs.Construct, cdkProps CDKStackProps) awscdk.Stack {
	props := cdkProps.StackProps
	stack := awscdk.NewStack(scope, aws.String("weatherapp"), &props)

	vpc := awsec2.NewVpc(stack, aws.String("weatherVpc"), &awsec2.VpcProps{
		MaxAzs: jsii.Number(2),
	})

	dataBucket := awss3.NewBucket(stack, aws.String("weatherDataBucket"), &awss3.BucketProps{
		BlockPublicAccess: awss3.BlockPublicAccess_BLOCK_ALL(),
		EnforceSSL:        aws.Bool(true),
		Versioned:         aws.Bool(false),
		Encryption:        awss3.BucketEncryption_S3_MANAGED,
		LifecycleRules: &[]*awss3.LifecycleRule{{
			AbortIncompleteMultipartUploadAfter: awscdk.Duration_Days(jsii.Number(7)),
			NoncurrentVersionExpiration:         awscdk.Duration_Days(jsii.Number(7)),
			Expiration:                          awscdk.Duration_Days(jsii.Number(7)),
		}},
		// TODO: add log bucket
		// ServerAccessLogsBucket: logBucket,
		// ServerAccessLogsPrefix: aws.String("s3logs/weatherDataImporter"),
	})

	weatherDataProcessingDLQ := awssqs.NewQueue(stack, jsii.String("weatherDataProcessorDLQ"), &awssqs.QueueProps{
		Encryption:      awssqs.QueueEncryption_SQS_MANAGED,
		EnforceSSL:      jsii.Bool(true),
		RetentionPeriod: awscdk.Duration_Days(jsii.Number(2)),
	})

	weatherDataProcessingQueue := awssqs.NewQueue(stack, jsii.String("weatherDataProcessorQueue"), &awssqs.QueueProps{
		Encryption:        awssqs.QueueEncryption_SQS_MANAGED,
		EnforceSSL:        jsii.Bool(true),
		VisibilityTimeout: awscdk.Duration_Minutes(jsii.Number(15)),
		DeadLetterQueue: &awssqs.DeadLetterQueue{
			MaxReceiveCount: jsii.Number(10),
			Queue:           weatherDataProcessingDLQ,
		},
	})

	// this is the lambda function that will get invoked when a new file is created in the bucket.
	onWeatherDataReceivedHandler := awslambdago.NewGoFunction(stack, jsii.String("onWeatherDataReceivedHandler"), &awslambdago.GoFunctionProps{
		Runtime:      awslambda.Runtime_PROVIDED_AL2(),
		Architecture: awslambda.Architecture_ARM_64(),
		Entry:        jsii.String("../event/handlers/onweatherdatareceived"),
		Bundling: &awslambdago.BundlingOptions{
			GoBuildFlags: &[]*string{jsii.String(`-ldflags "-s -w" -tags lambda.norpc`)},
		},
		Environment: &map[string]*string{
			"WEATHER_DATA_BUCKET_NAME":   dataBucket.BucketName(),
			"WEATHER_DATA_SQS_QUEUE_URL": weatherDataProcessingQueue.QueueUrl(),
		},
		MemorySize: jsii.Number(1024),
		Tracing:    awslambda.Tracing_ACTIVE,
		Timeout:    awscdk.Duration_Millis(jsii.Number(60000)),
		Vpc:        vpc,
		VpcSubnets: &awsec2.SubnetSelection{
			SubnetType: awsec2.SubnetType_PRIVATE_WITH_EGRESS,
		},
	})
	dataBucket.GrantRead(onWeatherDataReceivedHandler, nil)
	dataBucket.AddObjectCreatedNotification(awss3notifications.NewLambdaDestination(onWeatherDataReceivedHandler),
		&awss3.NotificationKeyFilter{
			Suffix: jsii.String(".csv"),
			Prefix: jsii.String("weather-data"),
		})
	weatherDataProcessingQueue.GrantSendMessages(onWeatherDataReceivedHandler)

	// this is the lambda function that will get invoked when a new SQS message arrives.
	onMessageReceivedHandler := awslambdago.NewGoFunction(stack, jsii.String("onMessageReceivedHandler"), &awslambdago.GoFunctionProps{
		Runtime:      awslambda.Runtime_PROVIDED_AL2(),
		Architecture: awslambda.Architecture_ARM_64(),
		Entry:        jsii.String("../event/handlers/onmessagereceived"),
		Bundling: &awslambdago.BundlingOptions{
			GoBuildFlags: &[]*string{jsii.String(`-ldflags "-s -w" -tags lambda.norpc`)},
		},
		Environment: &map[string]*string{
			"WEATHER_API_ENDPOINT": cdkProps.WeatherAPIEndpoint,
			"WEATHER_API_KEY":      cdkProps.WeatherAPIKey,
		},
		MemorySize: jsii.Number(1024),
		Tracing:    awslambda.Tracing_ACTIVE,
		Timeout:    awscdk.Duration_Millis(jsii.Number(60000)),
		Vpc:        vpc,
	})
	onMessageReceivedHandler.AddEventSource(awslambdaeventsources.NewSqsEventSource(weatherDataProcessingQueue, &awslambdaeventsources.SqsEventSourceProps{
		BatchSize: jsii.Number(1),
	}))
	return stack
}

func main() {
	err := godotenv.Load("../.env")
	if err != nil {
		panic("Error loading .env file")
	}
	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		panic("AWS_REGIONn undefined")
	}
	awsAccount := os.Getenv("AWS_ACCOUNT_ID")
	if awsAccount == "" {
		panic("AWS_ACCOUNT_ID undefined")
	}
	weatherApiKey := os.Getenv("WEATHER_API_KEY")
	if weatherApiKey == "" {
		panic("WEATHER_API_KEY undefined")
	}
	weatherApiEndpoint := os.Getenv("WEATHER_API_ENDPOINT")
	if weatherApiEndpoint == "" {
		panic("WEATHER_API_ENDPOINT undefined")
	}

	app := awscdk.NewApp(nil)
	NewCDKStack(app, CDKStackProps{
		StackProps: awscdk.StackProps{
			Env: &awscdk.Environment{
				Region:  aws.String(awsRegion),
				Account: aws.String(awsAccount),
			},
		},
		WeatherAPIKey:      aws.String(weatherApiKey),
		WeatherAPIEndpoint: aws.String(weatherApiEndpoint),
	})
	app.Synth(nil)
}
