package com.okrean.kafka.topic.consumers.info;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class Main {
    private static final FastDateFormat DF = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        // kafka brokers - ip:port,ip:port...
        String brokers = "localhost:9092";
        String topic = "some_topic_name";

        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "OFF");
        if (StringUtils.isNotBlank(brokers) && StringUtils.isNotBlank(topic)) {
            Properties props = new Properties();
            props.put("bootstrap.servers", brokers);
            props.put("connections.max.idle.ms", 10000);
            props.put("request.timeout.ms", 5000);
            print("Trying to connect to brokers \"" + brokers + "\" !");
            try (AdminClient adminClient = KafkaAdminClient.create(props)) {
                ListTopicsResult listTopicsResult = adminClient.listTopics();
                Set<String> topics = listTopicsResult.names().get();
                print("Connection to brokers is OK!");

                boolean isTopicExists = topics.contains(topic);
                if (isTopicExists) {
                    print("Topic \"" + topic + "\" has found!");
                } else {
                    print("Can't find topic \"" + topic + "\"!");
                }

                DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topic));
                TopicDescription topicDescription = describeTopicsResult.allTopicNames().get().get(topic);
                print("Topic info: " + topicDescription.toString());

                ListConsumerGroupsResult listConsumerGroupsResult = adminClient.listConsumerGroups();
                List<String> allConsumerGroups = listConsumerGroupsResult.all().get().stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());

                for (String consumerGroup : allConsumerGroups) {
                    ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = adminClient.listConsumerGroupOffsets(consumerGroup);
                    Map<TopicPartition, OffsetAndMetadata> map = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
                    List<TopicPartition> topicPartitions = map.keySet().stream().filter(f -> f.topic().equalsIgnoreCase(topic)).collect(Collectors.toList());
                    for (TopicPartition partition : topicPartitions) {
                        OffsetAndMetadata offsetAndMetadata = map.get(partition);
                        print("For topic: " + partition.topic() + " with partition: " + partition.partition() + ", data: " + offsetAndMetadata);

                        Map<TopicPartition, OffsetSpec> topicPartitionOffsets = new HashMap<>();
                        topicPartitionOffsets.put(partition, OffsetSpec.latest());
                        ListOffsetsResult listOffsetsResult = adminClient.listOffsets(topicPartitionOffsets);
                        KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> listOffsetsResultInfoKafkaFuture = listOffsetsResult.partitionResult(partition);
                        ListOffsetsResult.ListOffsetsResultInfo listOffsetsResultInfo = listOffsetsResultInfoKafkaFuture.get();
                        print("Offset info: " + listOffsetsResultInfo);
                    }
                }
            } catch (Exception ex) {
                print(ex.getMessage());
            }
        }
    }

    private static void print(String text) {
        System.out.println(getNow() + ": " + text);
    }

    private static String getNow() {
        return DF.format(new Date());
    }
}
