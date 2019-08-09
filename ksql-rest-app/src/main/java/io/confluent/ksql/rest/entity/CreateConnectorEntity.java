/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateConnectorEntity extends KsqlEntity {

  private final ConnectorInfo info;

  @JsonCreator
  public CreateConnectorEntity(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("info") final ConnectorInfo info
  ) {
    super(statementText);
    this.info = Objects.requireNonNull(info, "info");
  }

  public ConnectorInfo getInfo() {
    return info;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final CreateConnectorEntity that = (CreateConnectorEntity) o;
    return Objects.equals(info, that.info);
  }

  @Override
  public int hashCode() {
    return Objects.hash(info);
  }

  @Override
  public String toString() {
    return "CreateConnectorEntity{"
        + "info=" + info
        + '}';
  }
}
