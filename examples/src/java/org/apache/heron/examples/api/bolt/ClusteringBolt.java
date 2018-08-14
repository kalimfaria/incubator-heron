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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.metric.GlobalMetrics;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.IUpdatable;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;

import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import jdk.nashorn.internal.objects.Global;

public class ClusteringBolt extends BaseRichBolt
    implements IUpdatable {
  private static Logger logger = Logger.getLogger(StatefulWordCountBolt.class.getName());

  private HashMap<String, Long> wordCountMap;
  private int taskID;
  private String componentId;
  Queue<DoublePoint> queue;
  final int QUEUE_SIZE = 30;
  KMeansPlusPlusClusterer<DoublePoint> clusterer;

  private OutputCollector outputCollector;

  public StatefulWordCountBolt() {
    super();

  }

  @SuppressWarnings("rawtypes")
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector aOutputCollector) {
    this.outputCollector = aOutputCollector;
    this.taskID = topologyContext.getThisTaskId();
    this.componentId = topologyContext.getThisComponentId();
    wordCountMap = new HashMap<>();
    queue = new LinkedList<DoublePoint>();
    // we would like to create 10 clusters, with max 100 iterations to create backpressure.
    // to do: consider making this a config parameter
    clusterer = new KMeansPlusPlusClusterer<DoublePoint>(10, 10);
  }

  @Override
  public void execute(Tuple tuple) {
    String key = "";
    Long time = -1L;

    // Now, we run a simple clustering algorithm to cluster the
    // lengths of the tuples into 10 different clusters, based on current
    // data in the queue.
    if (tuple.contains("word"))
      key = tuple.getStringByField("word");
    if (tuple.contains("time"))
      time = tuple.getLongByField("time");

    long latency = (System.currentTimeMillis() - time);
    GlobalMetrics.incrBy("end_to_end_latency", (int)latency);
    GlobalMetrics.incr("tuple_count");

    DoublePoint dataPoint = new DoublePoint(new int[] { 0,  key.length()});
    queue.add(dataPoint);

    while (queue.size() > QUEUE_SIZE)
      queue.remove();
    if (queue.size() == QUEUE_SIZE)
    {
      List<CentroidCluster<DoublePoint>> clusterResults = clusterer.cluster(queue);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    // do nothing
  }

  @Override
  public void update(org.apache.heron.api.topology.TopologyContext heronTopologyContext) {
    List<Integer> newTaskIds =
        heronTopologyContext.getComponentTasks(heronTopologyContext.getThisComponentId());
    System.out.println("Bolt updated with new topologyContext. New taskIds: " + newTaskIds);
  }
}
