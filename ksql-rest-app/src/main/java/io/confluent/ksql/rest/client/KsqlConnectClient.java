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

package io.confluent.ksql.rest.client;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.rest.entity.ConnectRequest;
import io.confluent.ksql.rest.entity.ConnectorInfo;
import io.confluent.ksql.rest.entity.ConnectorList;

import java.util.Map;
import java.util.Optional;

public class KsqlConnectClient {
  private final RestClient client;

  public KsqlConnectClient(final String serverAddress) {
    this.client = new RestClient(serverAddress);
  }

  @VisibleForTesting
  KsqlConnectClient(RestClient client) {
    this.client = client;
  }

  public RestResponse<ConnectorList> getConnectors() {
    return client.getRequest("/connectors", ConnectorList.class);
  }

  public RestResponse<ConnectorInfo> createNewConnector(final String name, final String topic) {
    final ConnectRequest jsonRequest = new ConnectRequest(name, topic);
    return client.postRequest("/connectors", jsonRequest, Optional.empty(), true,
        r -> r.readEntity(ConnectorInfo.class));
  }

  public RestResponse<ConnectorInfo> createNewConnector(
          final String name,
          Map<String, Object> config) {
    final ConnectRequest jsonRequest = new ConnectRequest(name, config);
    return client.postRequest("/connectors", jsonRequest, Optional.empty(), true,
        r -> r.readEntity(ConnectorInfo.class));
  }

  public RestResponse<ConnectorInfo> getConnectorInfo(final String connector) {
    return client.getRequest(String.format("/connectors/%s", connector), ConnectorInfo.class);
  }

  public RestResponse<String> deleteConnector(final String connector) {
    return client.deleteRequest(String.format("/connectors/%s", connector), String.class);
  }
}
