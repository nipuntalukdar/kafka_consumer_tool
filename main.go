package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"
)

var (
	defaultRetentionTime int64 = 36525 * 86400000 // almost 100 years
	valid_commands             = []string{"commitoffsets", "getoffsets", "listconsumers", "listgroups", "listtopics", "listbrokers"}
	brokers                    = flag.String("brokers", "127.0.0.1:9092", "Bootstrap brokers comma separated list")
	consumergroup              = flag.String("consumergroup", "testgroup", "consumer group name")
	inputjson                  = flag.String("inputjson", "topic_offsets.json",
		"JSON file containing the input details, needed for getoffsets and commitoffsets")
	command = flag.String("command", "getoffsets",
		fmt.Sprintf("command to execute, valid commands are: %s", strings.Join(valid_commands, ", ")))
	offsetRet = flag.Int64("offsetRetentionTime", defaultRetentionTime,
		"Offset retention time for commitoffsets in milliseconds")
	topics = flag.String("topics", "testtopic", "comma-separated list of topics for which offset will"+
		" be fetched, used when input json is not given and we want to fetch offset for all partitions")
)

type intArr [2]int64
type intArrs []intArr

type topicOffset struct {
	Topic   string
	Offsets intArrs
}

func (in intArrs) Len() int {
	return len(in)
}

func (in intArrs) Swap(i, j int) {
	in[i][0], in[i][1], in[j][0], in[j][1] = in[j][0], in[j][1], in[i][0], in[i][1]
}

func (in intArrs) Less(i, j int) bool {
	if in[i][0] != in[j][0] {
		return in[i][0] < in[j][0]
	}
	return in[i][1] < in[j][1]
}

type int32Arr []int32

func (in int32Arr) Len() int {
	return len(in)
}

func (in int32Arr) Swap(i, j int) {
	in[i], in[j] = in[j], in[i]
}

func (in int32Arr) Less(i, j int) bool {
	return in[i] < in[j]
}

func listGroups(client sarama.Client, conf *sarama.Config) bool {
	brokers := client.Brokers()
	ret := false
	for _, br := range brokers {
		if err := br.Open(conf); err != nil && err != sarama.ErrAlreadyConnected {
			log.Printf("Error in opening broker %v, %v", br, err)
			continue
		}
		groups, err := br.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil || groups.Err != sarama.ErrNoError {
			br.Close()
			continue
		}
		ret = true
		for group, _ := range groups.Groups {
			log.Printf("Group: %s\n", group)
		}
		br.Close()
	}
	return ret
}

func listTopics(client sarama.Client) bool {
	topics, err := client.Topics()
	if err != nil {
		log.Printf("Couldn't list topics: %v", err)
		return false
	}
	for _, topic := range topics {
		log.Printf("Topic:%s \n", topic)
	}
	return true
}

func listBrokers(client sarama.Client) bool {
	brokers := client.Brokers()
	for _, broker := range brokers {
		log.Printf("Broker Id:%d, addr:%s  \n", broker.ID(), broker.Addr())
	}
	return true
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
	var topics []string
	var partitions int32Arr
	for k, _ := range resp.Blocks {
		topics = append(topics, k)
	}
	sort.Strings(topics)
	log.Print("Current offset details:")
	for _, topic := range topics {
		for partition, _ := range resp.Blocks[topic] {
			partitions = append(partitions, partition)
		}
		sort.Sort(partitions)
		for _, partition := range partitions {
			log.Printf("Topic: %s, Partition: %d, Offset: %d", topic, partition,
				resp.Blocks[topic][partition].Offset)
		}
		partitions = partitions[:0]
	}
}

func main() {
	flag.Parse()
	var poffsets []topicOffset
	getalloffsets := false
	if *command != "listconsumers" {
		_, err := os.Stat(*inputjson)
		if os.IsNotExist(err) {
			if *command != "getoffsets" {
				log.Fatal("Input json is required")
			}
			getalloffsets = true
		} else {
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
		if getalloffsets {
			topic_list := strings.Split(*topics, ",")
			mdr, err := broker.GetMetadata(&sarama.MetadataRequest{Topics: topic_list})
			if err != nil {
				log.Fatalf("Unable to fetch metadata for topic %s", topics)
			}
			var t topicOffset
			for _, tmdr := range mdr.Topics {
				if tmdr.Err != sarama.ErrNoError {
					log.Printf("Couldn't fetch metadata for %s", tmdr.Name)
					continue
				}
				t.Topic = tmdr.Name
				t.Offsets = []intArr{}
				for _, pmdr := range tmdr.Partitions {
					if pmdr.Err != sarama.ErrNoError {
						log.Printf("Some error for partition %d for topic %s", pmdr.ID, t.Topic)
						continue
					}
					t.Offsets = append(t.Offsets, [2]int64{int64(pmdr.ID), 0})
				}
				if len(t.Offsets) > 0 {
					sort.Sort(t.Offsets)
					poffsets = append(poffsets, t)
				}
			}
		}
		if len(poffsets) == 0 {
			log.Fatalf("Empty topic or partition list for getting offsets")
		}
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
	} else if *command == "commitoffsets" {
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
	} else if *command == "listgroups" {
		listGroups(client, conf)
	} else if *command == "listtopics" {
		listTopics(client)
	} else if *command == "listbrokers" {
		listBrokers(client)
	}
}
