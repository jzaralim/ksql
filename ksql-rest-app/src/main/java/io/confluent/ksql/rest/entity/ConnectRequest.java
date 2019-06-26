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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSubTypes({})
public class ConnectRequest {
  private final String name;
  private final Map<String, Object> config;

  public ConnectRequest(final String name, final String topic) {
    this(name, cassandraConfigBuilder(name, topic));
  }

  @JsonCreator
  public ConnectRequest(
          @JsonProperty("name") final String name,
          @JsonProperty("config") final Map<String, Object> config
  ) {
    this.name = name;
    this.config = config;
  }

  public String getName() {
    return name;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof ConnectRequest)) {
      return false;
    }

    final ConnectRequest that = (ConnectRequest) o;
    return Objects.equals(name, that.name)
            && Objects.equals(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, config);
  }

  private static Map<String, Object> cassandraConfigBuilder(final String name, final String topic) {
    final Map<String, Object> redisConfig = new HashMap<String, Object>();
    redisConfig.put("name", name);
    redisConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    //redisConfig.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
    //redisConfig.put("key.converter", "io.confluent.connect.avro.AvroConverter");
    //redisConfig.put("key.converter.schemas.enable", "false");
    //redisConfig.put("key.converter.schema.registry.url", "http://localhost:8081");
    redisConfig.put("value.converter", "io.confluent.connect.avro.AvroConverter");
    redisConfig.put("value.converter.schema.registry.url", "http://localhost:8081");
    redisConfig.put("transforms", "ValueToKey");
    redisConfig.put("transforms.ValueToKey.type", "org.apache.kafka.connect.transforms.ValueToKey");
    redisConfig.put("transforms.ValueToKey.fields", "USERID,PAGEID");
    redisConfig.put("connector.class",
            "io.confluent.connect.cassandra.CassandraSinkConnector");
    redisConfig.put("tasks.max", 1);
    redisConfig.put("topics", topic);
    redisConfig.put("cassandra.contact.points", "localhost");
    redisConfig.put("cassandra.keyspace", name);
    redisConfig.put("cassandra.write.mode", "Insert");
    redisConfig.put("cassandra.consistency.level", "ANY");
    return redisConfig;
  }
}
