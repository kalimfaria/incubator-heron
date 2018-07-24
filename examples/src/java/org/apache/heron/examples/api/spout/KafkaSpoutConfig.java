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

import java.io.Serializable;

import org.apache.heron.api.spout.Scheme;

/**
 * Config to be consumed by kafka spout
 *
 * @param topic the kafka topic that spout will consume from
 * @param groupId the identifier for a group of consumers
 * @param scheme scheme used to convert kafka raw byte message to tuples
 * @param zkString zookeeper address to discover brokers' host:port
 * @param brokerPath broker path in zookeeper
 * @param pollTimeoutMs timeout for a kafka consumer polling
 * @param maxFetchSizeInBytes max data size for each poll
 */
public class KafkaSpoutConfig implements Serializable {
  public String topic;

  public String groupId;

  public Scheme scheme;

  public String zkString;

  public String brokerPath;

  public long pollTimeoutMs = 100;

  public int maxFetchSizeInBytes = 10 * 1024 * 1024; // 10MB

  // TODO(nlu): add more required configs
}
