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
import com.fasterxml.jackson.annotation.JsonSubTypes;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSubTypes({})
public class ConnectorTask {
  private String connector;
  private int task;

  @JsonCreator
  public ConnectorTask(
          @JsonProperty("connector") final String connector,
          @JsonProperty("task") final int task
  ) {
    this.connector = connector;
    this.task = task;
  }

  public String getConnector() {
    return connector;
  }

  public int getTask() {
    return task;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ConnectorTask that = (ConnectorTask) o;
    return Objects.equals(connector, that.connector) && Objects.equals(task, that.task);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connector, task);
  }
}
