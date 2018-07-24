//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.
package org.apache.heron.examples.api.spout;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.heron.api.spout.IRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Prototype Kafka Spout for kafka 0.10.X and above with stateful support.
 */
public class KafkaSpout implements
    IRichSpout, IStatefulComponent<TopicPartition, OffsetAndMetadata> {
  private static Logger logger = Logger.getLogger(KafkaSpout.class.getName());

  private static final int INITIAL_BROKER_NUMBER = 5;
  private static final int MAX_RETRY_TIME = 5;
  private static final int SLEEP_MS_BETWEEN_RETRY = 1000;
  private static final int SESSION_TIMEOUT_MS = 1000;
  private static final int CONNECTION_TIMEOUT_MS = 1000;

  private Map<String, Object> config;
  private TopologyContext context;
  private SpoutOutputCollector collector;

  private KafkaSpoutConfig kafkaSpoutConfig;
  // the kafka consumer used to poll data from kafka cluster
  private KafkaConsumer<ByteBuffer, ByteBuffer> consumer;

  private int myTaskId;
  private List<Integer> allTaskIds;

  // Record fetched from kafka to be processed later
  private Queue<ConsumerRecord<ByteBuffer, ByteBuffer>> bufferedMessages;
  // Message waiting to be acked
  private Map<UUID, ConsumerRecord<ByteBuffer, ByteBuffer>> pendingMessages;
  // map of partition to its corresponding offset
  private Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap;

  public KafkaSpout(KafkaSpoutConfig kafkaSpoutConfig) {
    this.kafkaSpoutConfig = kafkaSpoutConfig;

    this.bufferedMessages = new LinkedList<>();
    this.pendingMessages = new HashMap<>();

    this.partitionOffsetMap = new HashMap<>();
  }

  @Override
  public void open(Map<String, Object> conf,
                   TopologyContext aContext,
                   SpoutOutputCollector aCollector) {
    this.config = conf;
    this.context = aContext;
    this.collector = aCollector;

    this.allTaskIds = aContext.getComponentTasks(aContext.getThisComponentId());
    this.myTaskId = this.allTaskIds.get(aContext.getThisTaskIndex());

    // fetch broker address from zk first
    List<String> brokerList = new LinkedList<>();
    fetchBrokers(brokerList);

    // setup kafka consumer
    setupConsumer(brokerList);
  }

  private void setupConsumer(List<String> brokerList) {
    // prepare and create kafka consume
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        String.join(";", brokerList));
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaSpoutConfig.groupId);
    props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
        kafkaSpoutConfig.maxFetchSizeInBytes);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteBufferDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteBufferDeserializer.class);

    this.consumer = new KafkaConsumer<ByteBuffer, ByteBuffer>(props);

    // we manually assign partitions
    assignPartitions();
  }

  private void fetchBrokers(List<String> brokerList) {
    try (CuratorFramework curatorClient =
             CuratorFrameworkFactory.newClient(kafkaSpoutConfig.zkString,
                 SESSION_TIMEOUT_MS,
                 CONNECTION_TIMEOUT_MS,
                 new RetryNTimes(MAX_RETRY_TIME, SLEEP_MS_BETWEEN_RETRY))) {
      // Start the client first
      curatorClient.start();
      List<String> children = curatorClient.getChildren().forPath(kafkaSpoutConfig.brokerPath);
      // only a subset of brokers' addresses are needed to access kafka cluster
      // once connected, all remaining brokers will be know automatically
      for (int i = 0; i < INITIAL_BROKER_NUMBER && i < children.size(); i++) {
        String path = kafkaSpoutConfig.brokerPath + "/" + children.get(i);
        logger.info("Logging the path: " + path);
        byte[] data = curatorClient
            .getData()
            .forPath(path);
        String addr = parseAddr(data);
        logger.info("added new broker: " + addr);
        brokerList.add(addr);
      }
    } catch (Exception ex) {
      throw new RuntimeException("failed to fetch brokers from zookeeper. ", ex);
    }
  }

  private String parseAddr(byte[] data) {
    try {
      JSONObject jsonObject = new JSONObject(new String(data));

      String host = (String) jsonObject.get("host");
      int port = (Integer) jsonObject.get("port");

      return host + ":" + port;
    } catch (JSONException ex) {
      throw new RuntimeException("failed to parse address data. ", ex);
    }
  }

  private void assignPartitions() {
    List<PartitionInfo> pInfos = consumer.partitionsFor(kafkaSpoutConfig.topic);
    List<TopicPartition> partitions = new LinkedList<>();
    int index = 0;

    // assign partitions to spouts in a round robin fashion
    for (PartitionInfo pInfo : pInfos) {
      if (index >= allTaskIds.size()) {
        index = 0;
      }
      if (allTaskIds.get(index) == myTaskId) {
        partitions.add(new TopicPartition(kafkaSpoutConfig.topic, pInfo.partition()));
      }

      index++;
    }

    logger.info("assigned partitions: " + partitions.toString());
    consumer.assign(partitions);

    // previously assigned some partitions, need to reset offset
    if (!partitionOffsetMap.isEmpty()) {
      for (Map.Entry<TopicPartition, OffsetAndMetadata>
          partitionOffset : partitionOffsetMap.entrySet()) {
        logger.info("setting partition: " + partitionOffset.getKey()
            + " ; offset: " + partitionOffset.getValue());
        consumer.seek(partitionOffset.getKey(), partitionOffset.getValue().offset());
      }
    }
  }

  @Override
  public void close() {
    if (consumer != null) {
      consumer.close();
    }
  }

  @Override
  public void nextTuple() {
    if (bufferedMessages.isEmpty()) {
      //TODO(nlu): create a separate thread for polling from kafka into a concurrent queue
      ConsumerRecords<ByteBuffer, ByteBuffer> records =
          consumer.poll(kafkaSpoutConfig.pollTimeoutMs);

      for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
        bufferedMessages.add(record);
      }
    }
    if (!bufferedMessages.isEmpty()) {
      UUID messageId = UUID.randomUUID();
      ConsumerRecord<ByteBuffer, ByteBuffer> record = bufferedMessages.poll();

      List<Object> tuple = deserializeRecord(record);
      collector.emit(tuple, messageId);

      pendingMessages.put(messageId, record);
    }
  }

  private List<Object> deserializeRecord(ConsumerRecord<ByteBuffer, ByteBuffer> record) {
    ByteBuffer value = record.value();
    byte[] payload = new byte[value.remaining()];

    value.get(payload);
    return kafkaSpoutConfig.scheme.deserialize(payload);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(kafkaSpoutConfig.scheme.getOutputFields());
  }

  @Override
  public void ack(Object msgId) {
    UUID messageId = (UUID) msgId;
    ConsumerRecord<ByteBuffer, ByteBuffer> record = pendingMessages.remove(messageId);

    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());

    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1);
    partitionOffsetMap.put(topicPartition, offsetAndMetadata);
  }

  @Override
  public void fail(Object msgId) {
    // TODO(nlu): add max # of retry logic
    ConsumerRecord<ByteBuffer, ByteBuffer> record = pendingMessages.remove(msgId);
    bufferedMessages.add(record);
  }

  @Override
  public void activate() {
    // TODO(nlu): to be implemented
  }

  @Override
  public void deactivate() {
    // TODO(nlu): to be implemented
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public void initState(State<TopicPartition, OffsetAndMetadata> state) {
    // Set the partition & offset correctly
    logger.info("initing the state...");
    this.partitionOffsetMap = state;
  }

  @Override
  public void preSave(String checkpointId) {
    logger.info("processing with checkpointId: " + checkpointId
        + " ; commit partition offset: " + partitionOffsetMap.toString());
    try {
      consumer.commitSync(partitionOffsetMap);
    } catch (Exception ex) {
      logger.severe("error when committing kafka offset: " + ex);
      throw new RuntimeException("failed to commit offsets to kafka", ex);
    }
  }
}
