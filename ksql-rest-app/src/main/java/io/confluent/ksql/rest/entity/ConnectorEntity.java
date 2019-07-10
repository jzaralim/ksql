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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorEntity extends KsqlEntity {

  private final ConnectorInfo connectorInfo;

  @JsonCreator
  public ConnectorEntity(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("connector") final ConnectorInfo connectorInfo
  ) {
    super(statementText);
    this.connectorInfo = connectorInfo;
  }

  public ConnectorInfo getConnectorInfo() {
    return connectorInfo;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ConnectorEntity)) {
      return false;
    }
    final ConnectorEntity that = (ConnectorEntity) o;
    return Objects.equals(getConnectorInfo(), that.getConnectorInfo());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getConnectorInfo());
  }
}