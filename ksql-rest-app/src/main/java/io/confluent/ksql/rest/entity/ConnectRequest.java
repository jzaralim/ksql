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
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.util.KsqlConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectRequest {
  private final String name;
  private final Map<String, Object> config;

  public ConnectRequest(
      final String name,
      final KsqlTable<?> dataSource,
      final KsqlConfig ksqlConfig) {
    this.name = name;
    this.config = cassandraConfigBuilder(name, dataSource, ksqlConfig);
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

  private Map<String, Object> cassandraConfigBuilder(
      final String name,
      final KsqlTable<?> dataSource,
      final KsqlConfig ksqlConfig) {
    final Map<String, String> properties = ksqlConfig.getAllConfigPropsWithSecretsObfuscated();
    final Map<String, Object> config = new HashMap<String, Object>();

    config.put("name", name);
    config.put("cassandra.keyspace", name);
    config.put("topics", dataSource.getKafkaTopicName());
    config.put("tasks.max", 1);
    config.put("connector.class",
        "io.confluent.connect.cassandra.CassandraSinkConnector");
    config.put("cassandra.consistency.level", "ANY");
    config.put("cassandra.write.mode", "Insert");
    config.put("cassandra.contact.points", properties.get(KsqlConfig.CASSANDRA_HOST_PROPERTY));
    config.put("cassandra.port", properties.get(KsqlConfig.CASSANDRA_PORT_PROPERTY));

    if (dataSource.getValueSerdeFactory().getFormat() == Format.JSON) {
      config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
      config.put("value.converter.schemas.enable", "false");
    } else if (dataSource.getValueSerdeFactory().getFormat() == Format.AVRO) {
      config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
      config.put("value.converter.schema.registry.url",
          properties.get(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY));
    } else {
      config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    }

    config.put("transforms", "KeyToValueTransform");
    config.put("transforms.KeyToValueTransform.type",
        "com.github.jzaralim.kafka.connect.transform.keytovalue.KeyToValueTransform");


    config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");

    return config;
  }
}