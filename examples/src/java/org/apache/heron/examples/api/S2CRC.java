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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronSubmitter;
import org.apache.heron.api.bolt.BaseBasicBolt;
import org.apache.heron.api.bolt.BasicOutputCollector;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.metric.GlobalMetrics;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.common.basics.ByteAmount;


public final class S2CRC {
  private S2CRC () {

  }

  // Utils class to generate random String at given length
  public static class RandomString {
    private final char[] symbols;

    private final Random random = new Random();

    private final char[] buf;

    public RandomString(int length) {
      // Construct the symbol set
      StringBuilder tmp = new StringBuilder();
      for (char ch = '0'; ch <= '9'; ++ch) {
        tmp.append(ch);
      }

      for (char ch = 'a'; ch <= 'z'; ++ch) {
        tmp.append(ch);
      }

      symbols = tmp.toString().toCharArray();
      if (length < 1) {
        throw new IllegalArgumentException("length < 1: " + length);
      }

      buf = new char[length];
    }

    public String nextString() {
      for (int idx = 0; idx < buf.length; ++idx) {
        buf[idx] = symbols[random.nextInt(symbols.length)];
      }

      return new String(buf);
    }
  }

  /**
   * A spout that emits a random word
   */
  public static class FastRandomSentenceSpout extends BaseRichSpout {
    private static final long serialVersionUID = 8068075156393183973L;

    private static final int ARRAY_LENGTH = 128 * 1024;
    private static final int WORD_LENGTH = 20;
    private static final int SENTENCE_LENGTH = 10;

    // Every sentence would be words generated randomly, split by space
    private final String[] sentences = new String[ARRAY_LENGTH];

    private final Random rnd = new Random(31);

    private SpoutOutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields("sentence", "time"));
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
      RandomString randomString = new RandomString(WORD_LENGTH);
      for (int i = 0; i < ARRAY_LENGTH; i++) {
        StringBuilder sb = new StringBuilder(randomString.nextString());
        for (int j = 1; j < SENTENCE_LENGTH; j++) {
          sb.append(" ");
          sb.append(randomString.nextString());
        }
        sentences[i] = sb.toString();
      }

      collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      int nextInt = rnd.nextInt(ARRAY_LENGTH);
      collector.emit(new Values(sentences[nextInt], System.currentTimeMillis()));
    }
  }

  public static class SplitSentence extends BaseRichBolt {
    private static final long serialVersionUID = 1249629174039601217L;

    private OutputCollector collector;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      String sentence = tuple.getString(0);

      Long time = -1L;
      if (tuple.contains("time"))
        time = tuple.getLongByField("time");

      for (String word : sentence.split("\\s+")) {
        collector.emit(new Values(word, 1, time));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count", "time"));
    }
  }

  public static class WordCount extends BaseRichBolt {
    private static final long serialVersionUID = -8492566595062774310L;

    private Map<String, Integer> counts = new HashMap<String, Integer>();
    private OutputCollector collector;


    @SuppressWarnings("rawtypes")
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector aOutputCollector) {
      this.collector = aOutputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

      Long time = -1L;
      if (tuple.contains("time"))
        time = tuple.getLongByField("time");

      long latency = (System.currentTimeMillis() - time);
      GlobalMetrics.incrBy("end_to_end_latency", (int)latency);
      GlobalMetrics.incr("tuple_count");

      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null) {
        count = 0;
      }
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static void main(String[] args) throws Exception {
    String name = "fast-word-count-topology";
    if (args != null && args.length > 0) {
      name = args[0];
    }

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new FastRandomSentenceSpout(), 1);
    builder.setBolt("split", new SplitSentence(), 60).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 60).fieldsGrouping("split", new Fields("word"));

    Config conf = new Config();

    // component resource configuration
    conf.setComponentRam("spout", ByteAmount.fromMegabytes(1024));
    conf.setComponentRam("split", ByteAmount.fromMegabytes(1024));
    conf.setComponentRam("count", ByteAmount.fromMegabytes(1024));

    // container resource configuration
    conf.setContainerDiskRequested(ByteAmount.fromGigabytes(3));
//    conf.setContainerRamRequested(ByteAmount.fromGigabytes(3));
    conf.setContainerCpuRequested(1);

    conf.setNumStmgrs(60);

    HeronSubmitter.submitTopology(name, conf, builder.createTopology());
  }
}
