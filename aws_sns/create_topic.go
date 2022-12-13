package aws_sns

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"log"
)

// SNSCreateTopicAPI defines the interface for the CreateTopic function.
// We use this interface to test the function using a mocked service.
type SNSCreateTopicAPI interface {
	CreateTopic(ctx context.Context,
		params *sns.CreateTopicInput,
		optFns ...func(*sns.Options)) (*sns.CreateTopicOutput, error)
}

// MakeTopic creates an Amazon Simple Notification Service (Amazon SNS) topic.
// Inputs:
//
//	c is the context of the method call, which includes the AWS Region.
//	api is the interface that defines the method call.
//	input defines the input arguments to the service call.
//
// Output:
//
//	If success, a CreateTopicOutput object containing the result of the service call and nil.
//	Otherwise, nil and an error from the call to CreateTopic.
func MakeTopic(c context.Context, api SNSCreateTopicAPI, input *sns.CreateTopicInput) (*sns.CreateTopicOutput, error) {
	return api.CreateTopic(c, input)
}

func main() {
	err := CreateTopic("a")
	if err != nil {
		fmt.Println("error")
	}
}

// CreateTopic TODO: change to real config ()
func CreateTopic(tidbClusterID string) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic("configuration error, " + err.Error())
	}
	client := sns.NewFromConfig(cfg)

	input := &sns.CreateTopicInput{
		Name: &tidbClusterID,
	}

	results, err := MakeTopic(context.TODO(), client, input)
	if err != nil {
		log.Printf("[error]Create topic failed, err: %+v\n", err.Error())
		return err
	}

	fmt.Println(*results.TopicArn)

	return nil
}

func PublishTopo(tidbClusterID string, topoList []string) error {
	return nil
}
