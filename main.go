package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
	"strings"
)

var (
	defaultRetentionTime int64 = 36525 * 86400000 // almost 100 years
	valid_commands             = []string{"commitoffsets", "getoffsets", "listconsumers"}
	brokers                    = flag.String("brokers", "127.0.0.1:9092", "Bootstrap brokers comma separated list")
	consumergroup              = flag.String("consumergroup", "testgroup", "consumer group name")
	inputjson                  = flag.String("inputjson", "topic_offsets.json",
		"JSON file containing the input details, needed for getoffsets and commitoffsets")
	command = flag.String("command", "getoffsets",
		fmt.Sprintf("command to execute, valid commands are: %s", strings.Join(valid_commands, ", ")))
	offsetRet = flag.Int64("offsetRetentionTime", defaultRetentionTime,
		"Offset retention time for commitoffsets in milliseconds")
)

type intArr [2]int64

type topicOffset struct {
	Topic   string
	Offsets []intArr
}

func getGroupJoinRequest(consumerGrop string) *sarama.JoinGroupRequest {
	ret := &sarama.JoinGroupRequest{}
	ret.GroupId = consumerGrop
	ret.SessionTimeout = 10000
	ret.ProtocolType = "consumer"
	ret.OrderedGroupProtocols = []*sarama.GroupProtocol{{Name: "consumer", Metadata: []byte{0, 0, 0, 3, 0x01, 0x02, 0x03}}}
	return ret
}

func printOffsetCommitResponse(offcr *sarama.OffsetCommitResponse) {
	for topic, offsets := range offcr.Errors {
		for part, err := range offsets {
			if err != sarama.ErrNoError {
				log.Printf("Topic: %s, Partition: %d, error: %s", topic, part, err)
			} else {
				log.Printf("Topic: %s, Partition: %d,  Offset committed successfully", topic, part)
			}
		}
	}
}

func commitOffsets(broker *sarama.Broker, consumerGroup string, generationId int32, consumerId string,
	topicoffsets []topicOffset) {
	log.Print("Commiting offsets")
	offcr := &sarama.OffsetCommitRequest{ConsumerGroup: consumerGroup, ConsumerID: consumerId,
		ConsumerGroupGeneration: generationId, Version: 2, RetentionTime: *offsetRet}
	for _, offsetdata := range topicoffsets {
		for _, off := range offsetdata.Offsets {
			offcr.AddBlock(offsetdata.Topic, int32(off[0]), int64(off[1]), 0, "")
		}
	}
	offcrresp, err := broker.CommitOffset(offcr)
	if err != nil {
		log.Fatalf("Error %v", err)
	}

	printOffsetCommitResponse(offcrresp)
}

func printOffsetFetchResponse(resp *sarama.OffsetFetchResponse) {
	if resp == nil {
		return
	}
	log.Print("Current offset details:")
	for topic, offpartions := range resp.Blocks {
		for part, offr := range offpartions {
			log.Printf("Topic: %s, Partition: %d, Offset: %d", topic, part, offr.Offset)
		}

	}
}

func main() {
	flag.Parse()
	var poffsets []topicOffset
	if *command != "listconsumers" {
		file_data, err := ioutil.ReadFile(*inputjson)
		if err != nil {
			log.Fatalf("File read, %v", err)
		}
		if !validate(file_data) {
			log.Fatal("Error validating json")
		}
		err = json.Unmarshal(file_data, &poffsets)
		if err != nil {
			log.Fatalf("Error decoding json file %v", err)
		}
	}
	broker_list := strings.Split(*brokers, ",")
	conf := sarama.NewConfig()
	conf.Version = sarama.V0_10_1_0
	client, err := sarama.NewClient(broker_list, conf)
	if err != nil {
		log.Fatal(err)
	}
	broker, err := client.Coordinator(*consumergroup)
	if err != nil {
		log.Fatal(err)
	}
	connected, err := broker.Connected()
	if err != nil {
		log.Fatalf("Broker connecting, %v", err)
	}
	if !connected {
		err = broker.Open(conf)
		if err != nil {
			log.Fatalf("Broker open %v", err)
		}
	}
	defer broker.Close()
	defer client.Close()

	descGrs := &sarama.DescribeGroupsRequest{[]string{*consumergroup}}
	resp, err := broker.DescribeGroups(descGrs)
	if err != nil {
		log.Fatalf("Error in getting consumer group details %v\n", err)
	}
	if *command == "listconsumers" {
		for _, groupdesc := range resp.Groups {
			log.Printf("Group: %s,  state: %s\n", groupdesc.GroupId, groupdesc.State)
			for id, desc := range groupdesc.Members {
				log.Printf("id: %s, host: %s, clientid: %s\n", id, desc.ClientHost, desc.ClientId)
			}
		}
		return
	}
	if *command == "commitoffsets" {
		if resp.Groups[0].State != "Empty" {
			log.Fatalf("Consumer group %s not empty, some consumers are running, stop them first ",
				*consumergroup)
		}
	}

	if *command == "getoffsets" {
		offr := &sarama.OffsetFetchRequest{ConsumerGroup: *consumergroup, Version: 1}
		for _, offsetdata := range poffsets {
			for _, off := range offsetdata.Offsets {
				offr.AddPartition(offsetdata.Topic, int32(off[0]))
			}
		}
		offsetdetails, err := broker.FetchOffset(offr)
		if err != nil {
			log.Fatal(err)
		}
		printOffsetFetchResponse(offsetdetails)
		return
	}
	if *command == "commitoffsets" {
		jr, err := broker.JoinGroup(getGroupJoinRequest(*consumergroup))
		if err != nil {
			log.Fatalf("Died %v", err)
		}
		_, err = broker.Heartbeat(&sarama.HeartbeatRequest{GroupId: *consumergroup, GenerationId: jr.GenerationId,
			MemberId: jr.MemberId})
		if err != nil {
			log.Fatalf("Died %v", err)
		}
		_, err = broker.SyncGroup(&sarama.SyncGroupRequest{GroupId: *consumergroup, GenerationId: jr.GenerationId, MemberId: jr.MemberId})
		if err != nil {
			log.Fatalf("Died %v", err)
		}
		commitOffsets(broker, *consumergroup, jr.GenerationId, jr.MemberId, poffsets)
	}
}
