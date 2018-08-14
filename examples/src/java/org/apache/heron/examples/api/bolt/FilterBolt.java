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
package org.apache.heron.examples.api.bolt;

import java.util.Map;
import java.util.List;

import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.IUpdatable;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.tuple.Fields;

public class FilterBolt extends BaseRichBolt implements IUpdatable {

  private static final long serialVersionUID = 1184860508880121352L;
  private long nItems;
  private long startTime;
  private OutputCollector collector;

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    String key = "";
    Long time = -1L;

    if (tuple.contains("word"))
      key = tuple.getStringByField("word");
    if (tuple.contains("time"))
      time = tuple.getLongByField("time");

    collector.emit(new Values(key, time));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "time"));
  }

  /**
   * Implementing this method is optional and only necessary if BOTH of the following are true:
   *
   * a.) you plan to dynamically scale your bolt/spout at runtime using 'heron update'.
   * b.) you need to take action based on a runtime change to the component parallelism.
   *
   * Most bolts and spouts should be written to be unaffected by changes in their parallelism,
   * but some must be aware of it. An example would be a spout that consumes a subset of queue
   * partitions, which must be algorithmically divided amongst the total number of spouts.
   * <P>
   * Note that this method is from the IUpdatable Heron interface which does not exist in Storm.
   * It is fine to implement IUpdatable along with other Storm interfaces, but implementing it
   * will bind an otherwise generic Storm implementation to Heron.
   *
   * @param heronTopologyContext Heron topology context.
   */
  @Override
  public void update(org.apache.heron.api.topology.TopologyContext heronTopologyContext) {
    List<Integer> newTaskIds =
        heronTopologyContext.getComponentTasks(heronTopologyContext.getThisComponentId());
    System.out.println("Bolt updated with new topologyContext. New taskIds: " + newTaskIds);
  }
}
