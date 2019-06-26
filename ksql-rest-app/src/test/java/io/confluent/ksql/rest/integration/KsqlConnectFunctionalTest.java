/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.integration;
import io.confluent.ksql.rest.client.KsqlConnectClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ConnectorInfo;
import io.confluent.ksql.rest.entity.ConnectorList;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class KsqlConnectFunctionalTest {
  private EmbeddedConnectCluster connect;
  private KsqlConnectClient client;
  private RestServer server;
  private Map<String, Object> testConnectorConfig;

  @SuppressWarnings("deprecation")
  @Before
  public void setup() throws IOException {
    testConnectorConfig = new HashMap<>();
    testConnectorConfig.put("connector.class",
            "org.apache.kafka.connect.integration.MonitorableSinkConnector");
    testConnectorConfig.put("tasks.max", 1);
    testConnectorConfig.put("topics", "topic");

    Map<String, String> workerProps = new HashMap<>();
    workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "status-topic");
    workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "config-topic");
    workerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    workerProps.put(DistributedConfig.GROUP_ID_CONFIG, "connect-test-group");
    workerProps.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
    workerProps.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
    workerProps.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
    workerProps.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
    workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
    workerProps.put(WorkerConfig.LISTENERS_CONFIG, "http://localhost:8080");
    DistributedConfig config = new DistributedConfig(workerProps);

    connect = new EmbeddedConnectCluster.Builder().name("ksql-connect-cluster").workerProps(workerProps).build();
    connect.start();
    server = new RestServer(config);
    client = new KsqlConnectClient(server.advertisedUrl().toString());
  }

  @After
  public void close() {
    connect.stop();
  }

  @Test
  public void testCreateConnector() {
    testConnectorConfig.put("name", "name");
    RestResponse<?> response =  client.createNewConnector("name", testConnectorConfig);
    assertThat(((ConnectorInfo) response.getResponse()).getName(), is("name"));
    assertThat(((ConnectorInfo) response.getResponse()).getConfig(), hasEntry("topics", "topic"));
    assertThat(((ConnectorInfo) response.getResponse()).getConfig(), hasEntry("tasks.max", "1"));
    response = client.getConnectors();
    assertThat(((ConnectorList) response.getResponse()).size(), is(1));
    client.deleteConnector("name");
    response = client.getConnectors();
    assertThat(((ConnectorList) response.getResponse()).size(), is(0));
  }
}
