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
package org.apache.pinot.core.realtime.impl.kafka.server;

import java.io.File;
import java.security.Permission;
import java.util.Properties;
import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.realtime.stream.StreamDataServerStartable;


public class KafkaDataServerStartable implements StreamDataServerStartable {
  private static final String PORT = "port";
  private static final String BROKER_ID = "brokerId";
  private static final String ZK_STR = "zkStr";
  private static final String LOG_DIR_PATH = "logDirPath";
  private static final int DEFAULT_TOPIC_PARTITION = 1;

  private KafkaServerStartable serverStartable;
  private String zkStr;
  private int port;
  private int brokerId;
  private String logDirPath;

  public static void configureSegmentSizeBytes(Properties properties, int segmentSize) {
    properties.put("log.segment.bytes", Integer.toString(segmentSize));
  }

  public static void configureLogRetentionSizeBytes(Properties properties, int logRetentionSizeBytes) {
    properties.put("log.retention.bytes", Integer.toString(logRetentionSizeBytes));
  }

  public static void configureKafkaLogDirectory(Properties configuration, File logDir) {
    configuration.put("log.dirs", logDir.getAbsolutePath());
  }

  public static void configureBrokerId(Properties configuration, int brokerId) {
    configuration.put("broker.id", Integer.toString(brokerId));
  }

  public static void configureZkConnectionString(Properties configuration, String zkStr) {
    configuration.put("zookeeper.connect", zkStr);
  }

  public static void configureKafkaPort(Properties configuration, int port) {
    configuration.put("port", Integer.toString(port));
  }

  public static void configureTopicDeletion(Properties configuration, boolean topicDeletionEnabled) {
    configuration.put("delete.topic.enable", Boolean.toString(topicDeletionEnabled));
  }

  public static void configureHostName(Properties configuration, String hostName) {
    configuration.put("host.name", hostName);
  }

  private static void invokeTopicCommand(String[] args) {
    // jfim: Use Java security to trap System.exit in Kafka 0.9's TopicCommand
    System.setSecurityManager(new SecurityManager() {
      @Override
      public void checkPermission(Permission perm) {
        if (perm.getName().startsWith("exitVM")) {
          throw new SecurityException("System.exit is disabled");
        }
      }

      @Override
      public void checkPermission(Permission perm, Object context) {
        checkPermission(perm);
      }
    });

    try {
      TopicCommand.main(args);
    } catch (SecurityException ex) {
      // Do nothing, this is caused by our security manager that disables System.exit
    }

    System.setSecurityManager(null);
  }

  public static void deleteTopic(String kafkaTopic, String zkStr) {
    invokeTopicCommand(new String[]{"--delete", "--zookeeper", zkStr, "--topic", kafkaTopic});
  }

  public void init(Properties props) {

    port = (Integer) props.get(PORT);
    brokerId = (Integer) props.get(BROKER_ID);
    zkStr = props.getProperty(ZK_STR);
    logDirPath = props.getProperty(LOG_DIR_PATH);

    // Create the ZK nodes for Kafka, if needed
    int indexOfFirstSlash = zkStr.indexOf('/');
    if (indexOfFirstSlash != -1) {
      String bareZkUrl = zkStr.substring(0, indexOfFirstSlash);
      String zkNodePath = zkStr.substring(indexOfFirstSlash);
      ZkClient client = new ZkClient(bareZkUrl);
      client.createPersistent(zkNodePath, true);
      client.close();
    }

    File logDir = new File(logDirPath);
    logDir.mkdirs();

    configureKafkaPort(props, port);
    configureZkConnectionString(props, zkStr);
    configureBrokerId(props, brokerId);
    configureKafkaLogDirectory(props, logDir);
    props.put("zookeeper.session.timeout.ms", "60000");
    KafkaConfig config = new KafkaConfig(props);

    serverStartable = new KafkaServerStartable(config);
  }

  @Override
  public void start() {
    serverStartable.startup();
  }

  @Override
  public void stop() {
    serverStartable.shutdown();
    FileUtils.deleteQuietly(new File(serverStartable.serverConfig().logDirs().apply(0)));
  }

  @Override
  public void createTopic(String topic, Properties props) {
    int partitionCount = DEFAULT_TOPIC_PARTITION;
    if (props.containsKey("partition")) {
      partitionCount = (Integer) props.get("partition");
    }
    invokeTopicCommand(
        new String[]{"--create", "--zookeeper", this.zkStr, "--replication-factor", "1", "--partitions", Integer.toString(
            partitionCount), "--topic", topic});
  }
}
