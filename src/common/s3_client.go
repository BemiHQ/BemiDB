package common

import (
	"context"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Client struct {
	Config *CommonConfig
	S3     *s3.Client
}

func NewS3Client(Config *CommonConfig) *S3Client {
	var awsConfigOptions = []func(*awsConfig.LoadOptions) error{
		awsConfig.WithRegion(Config.Aws.Region),
	}

	if Config.LogLevel == LOG_LEVEL_TRACE {
		awsConfigOptions = append(awsConfigOptions, awsConfig.WithClientLogMode(aws.LogRequest))
	}

	if IsLocalHost(Config.Aws.S3Endpoint) {
		awsConfigOptions = append(awsConfigOptions, awsConfig.WithBaseEndpoint("http://"+Config.Aws.S3Endpoint))
	} else {
		awsConfigOptions = append(awsConfigOptions, awsConfig.WithBaseEndpoint("https://"+Config.Aws.S3Endpoint))
	}

	awsCredentials := credentials.NewStaticCredentialsProvider(
		Config.Aws.AccessKeyId,
		Config.Aws.SecretAccessKey,
		"",
	)
	awsConfigOptions = append(awsConfigOptions, awsConfig.WithCredentialsProvider(awsCredentials))

	loadedAwsConfig, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfigOptions...)
	PanicIfError(Config, err)

	s3 := s3.NewFromConfig(loadedAwsConfig, func(o *s3.Options) {
		if Config.Aws.S3Endpoint != DEFAULT_AWS_S3_ENDPOINT {
			o.UsePathStyle = true
		}
	})

	return &S3Client{
		Config: Config,
		S3:     s3,
	}
}

func (s3Client *S3Client) BucketS3Prefix() string {
	return "s3://" + s3Client.Config.Aws.S3Bucket + "/"
}

// s3://bucket/some/path -> some/path
func (s3Client *S3Client) ObjectKey(objectPath string) string {
	return strings.TrimPrefix(objectPath, s3Client.BucketS3Prefix())
}

func (s3Client *S3Client) HeadObject(fileKey string) *s3.HeadObjectOutput {
	output, err := s3Client.S3.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(s3Client.Config.Aws.S3Bucket),
		Key:    aws.String(fileKey),
	})
	PanicIfError(s3Client.Config, err)

	return output
}

func (s3Client *S3Client) DeleteObject(fileKey string) {
	_, err := s3Client.S3.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s3Client.Config.Aws.S3Bucket),
		Key:    aws.String(fileKey),
	})
	PanicIfError(s3Client.Config, err)
}

func (s3Client *S3Client) GetObject(fileKey string) *s3.GetObjectOutput {
	getObjectOutput, err := s3Client.S3.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s3Client.Config.Aws.S3Bucket),
		Key:    aws.String(fileKey),
	})
	PanicIfError(s3Client.Config, err)
	return getObjectOutput
}

func (s3Client *S3Client) UploadObject(fileKey string, file *os.File) {
	uploader := manager.NewUploader(s3Client.S3)
	_, err := uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s3Client.Config.Aws.S3Bucket),
		Key:    aws.String(fileKey),
		Body:   file,
	})
	PanicIfError(s3Client.Config, err)
}

func (s3Client *S3Client) ListObjects(prefix string) *s3.ListObjectsV2Output {
	listObjectsOutput, err := s3Client.S3.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Client.Config.Aws.S3Bucket),
		Prefix: aws.String(prefix),
	})
	PanicIfError(s3Client.Config, err)

	return listObjectsOutput
}

func (s3Client *S3Client) DeleteObjects(fileKeys []*string) {
	objectsToDelete := make([]types.ObjectIdentifier, len(fileKeys))
	for i, fileKey := range fileKeys {
		objectsToDelete[i] = types.ObjectIdentifier{Key: fileKey}
	}

	_, err := s3Client.S3.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
		Bucket: aws.String(s3Client.Config.Aws.S3Bucket),
		Delete: &types.Delete{
			Objects: objectsToDelete,
			Quiet:   aws.Bool(true),
		},
	})
	PanicIfError(s3Client.Config, err)
}
