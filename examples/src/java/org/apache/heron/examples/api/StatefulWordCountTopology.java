/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.examples.api;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronSubmitter;
import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.exception.AlreadyAliveException;
import org.apache.heron.api.exception.InvalidTopologyException;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.Scheme;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.examples.api.WordCountScheme;
import org.apache.heron.examples.api.spout.KafkaSpout;
import org.apache.heron.examples.api.spout.KafkaSpoutConfig;

/**
 * This is a topology that does simple word counts.
 * <p>
 * In this topology,
 * 1. the spout task generate a set of random words during initial "open" method.
 * (~128k words, 20 chars per word)
 * 2. During every "nextTuple" call, each spout simply picks a word at random and emits it
 * 3. Spouts use a fields grouping for their output, and each spout could send tuples to
 * every other bolt in the topology
 * 4. Bolts maintain an in-memory map, which is keyed by the word emitted by the spouts,
 * and updates the count when it receives a tuple.
 * The difference between this topology and the StatefulWordCountTopology is that this topology
 * has some stateful components. In particular the bolt remembers the count in the
 * state variable. This way the counts are guaranteed to be accurate regardless of
 * spouts/bolt/system failures.
 */

public final class StatefulWordCountTopology {
  static KafkaSpoutConfig sourceConfig;
  static String SPOUT_NAME = "kafka_spout";
  static String BOLT_NAME = "count_bolt";
  private StatefulWordCountTopology() throws Exception {
    // do nothing
  }

  /***
   * examples for construct and submit a stateful wordcount topology
   * @param args topology name
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new RuntimeException("missing topology name");
    }

    final String TOPOLOGY_NAME = args[0];

    sourceConfig = new KafkaSpoutConfig();
    sourceConfig.scheme = new WordCountScheme();
    sourceConfig.zkString = "zk-local-msgeventbus.smf1.twitter.com:2181";
    sourceConfig.brokerPath = "/messaging/kafka/prod/cdl-testing-1/brokers/ids";
    sourceConfig.groupId = "kafka-spout-" + System.currentTimeMillis();
    sourceConfig.topic = "word-count-heron";

    int parallelism = 1;

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_NAME, new KafkaSpout(sourceConfig), parallelism);
    builder.setBolt(BOLT_NAME, new StatefulWordCountBolt(), parallelism)
        .fieldsGrouping(SPOUT_NAME, new Fields("word"));

    Config conf = new Config();
    // set the topology to stateful
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE);
    // checkpoint internal
    conf.setTopologyStatefulCheckpointIntervalSecs(60);
    // clean the previous saved state every time the topology is restarted
    conf.setTopologyStatefulStartClean(true);
    // configure number of containers
    conf.setNumStmgrs(parallelism);

    // configure components' resource
    conf.setComponentRam(SPOUT_NAME,
        ByteAmount.fromGigabytes(1));
    conf.setComponentRam(BOLT_NAME,
        ByteAmount.fromGigabytes(1));

    // cofigure container resources
    conf.setContainerDiskRequested(
        ByteAmount.fromGigabytes(2));
    conf.setContainerRamRequested(
        ByteAmount.fromGigabytes(3));
    conf.setContainerCpuRequested(3);

    HeronSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());


  }



}
