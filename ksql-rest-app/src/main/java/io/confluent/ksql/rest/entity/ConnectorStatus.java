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

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorStatus {
  private final String name;
  private final String type;
  private final Map<String, String> connector;
  private final List<Map<String, Object>> tasks;

  @JsonCreator
  public ConnectorStatus(
      @JsonProperty("name") final String name,
      @JsonProperty("type") final String type,
      @JsonProperty("connector") final Map<String, String> connector,
      @JsonProperty("tasks") final List<Map<String, Object>> tasks
  ) {
    this.name = name;
    this.type = type;
    this.connector = connector;
    this.tasks = tasks;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public Map<String, String> getConnector() {
    return connector;
  }

  public List<Map<String, Object>> getTasks() {
    return tasks;
  }
}
