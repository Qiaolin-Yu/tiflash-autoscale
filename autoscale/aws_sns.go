package autoscale

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"log"
)

type AwsSnsManager struct {
	topicArnMap map[string]string
	client      *sns.Client
}

type TopologyMessage struct {
	TidbClusterID string
	Timestamp     int64
	TopologyList  []string
}

// SNSCreateTopicAPI defines the interface for the CreateTopic function.
// We use this interface to test the function using a mocked service.
type SNSCreateTopicAPI interface {
	CreateTopic(ctx context.Context,
		params *sns.CreateTopicInput,
		optFns ...func(*sns.Options)) (*sns.CreateTopicOutput, error)
}

// SNSPublishAPI defines the interface for the Publish function.
// We use this interface to test the function using a mocked service.
type SNSPublishAPI interface {
	Publish(ctx context.Context,
		params *sns.PublishInput,
		optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
}

func NewAwsSnsManager(region string) *AwsSnsManager {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	snsClient := sns.NewFromConfig(cfg)
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	ret := &AwsSnsManager{
		topicArnMap: make(map[string]string),
		client:      snsClient,
	}
	return ret
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

func (c *AwsSnsManager) TryToPublishTopology(tidbClusterID string, timestamp int64, topologyList []string) error {
	_, exist := c.topicArnMap[tidbClusterID]
	if exist {
		return c.publishTopology(tidbClusterID, timestamp, topologyList)
	}
	err := c.createTopic(tidbClusterID)
	if err != nil {
		return err
	}
	return c.publishTopology(tidbClusterID, timestamp, topologyList)
}

func (c *AwsSnsManager) createTopic(tidbClusterID string) error {

	topicName := "tiflash_cns_of_" + tidbClusterID
	input := &sns.CreateTopicInput{
		Name: &topicName,
	}

	results, err := MakeTopic(context.TODO(), c.client, input)
	if err != nil {
		log.Printf("[error]Create topic failed, err: %+v\n", err.Error())
		return err
	}
	log.Printf("[CreateTopic]topic ARN: %v \n", *results.TopicArn)
	c.topicArnMap[tidbClusterID] = *results.TopicArn
	return nil
}

// PublishMessage publishes a message to an Amazon Simple Notification Service (Amazon SNS) topic
// Inputs:
//
//	c is the context of the method call, which includes the Region
//	api is the interface that defines the method call
//	input defines the input arguments to the service call.
//
// Output:
//
//	If success, a PublishOutput object containing the result of the service call and nil
//	Otherwise, nil and an error from the call to Publish
func PublishMessage(c context.Context, api SNSPublishAPI, input *sns.PublishInput) (*sns.PublishOutput, error) {
	return api.Publish(c, input)
}

func (c *AwsSnsManager) publishTopology(tidbClusterID string, timestamp int64, topologyList []string) error {

	topologyMessage := TopologyMessage{
		TidbClusterID: tidbClusterID,
		Timestamp:     timestamp,
		TopologyList:  topologyList,
	}
	topicARN := c.topicArnMap[tidbClusterID]
	jsonTopo, err := json.Marshal(topologyMessage)
	message := string(jsonTopo)

	input := &sns.PublishInput{
		Message:  &message,
		TopicArn: &topicARN,
	}

	result, err := PublishMessage(context.TODO(), c.client, input)
	if err != nil {
		log.Printf("[error]Publish topology failed, err: %+v\n", err.Error())
		return err
	}
	log.Printf("[PublishTopology]message ID: %v \n", *result.MessageId)
	return nil
}
