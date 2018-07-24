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
package org.apache.heron.examples.api;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;

public class StatefulWordCountBolt extends BaseRichBolt
    implements IStatefulComponent<String, Long> {
  private static Logger logger = Logger.getLogger(StatefulWordCountBolt.class.getName());

  private State<String, Long> wordCountMap;

  private OutputCollector outputCollector;

  public StatefulWordCountBolt() {
    super();
  }

  @SuppressWarnings("rawtypes")
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector aOutputCollector) {
    this.outputCollector = aOutputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    String key = tuple.getString(0);

    if (wordCountMap.get(key) == null) {
      wordCountMap.put(key, 1L);
    } else {
      Long val = wordCountMap.get(key);
      wordCountMap.put(key, ++val);
    }
    System.out.println(key);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    // do nothing
  }

  @Override
  public void initState(State<String, Long> state) {
    logger.info("restoring state: " + state.toString());
    this.wordCountMap = state;
  }

  @Override
  public void preSave(String checkpointId) {
    logger.info("presave for checkpoint id: " + checkpointId);
    logger.info("current state is: " + wordCountMap.toString());
  }
}
