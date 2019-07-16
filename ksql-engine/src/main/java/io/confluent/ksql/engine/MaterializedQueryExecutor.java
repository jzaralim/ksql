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

package io.confluent.ksql.engine;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SelectItem;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Field;

public class MaterializedQueryExecutor {
  private final CqlSession session;
  private final MetaStore metaStore;

  public MaterializedQueryExecutor(final String host, final int port, final MetaStore metaStore) {
    this.metaStore = metaStore;
    session = CqlSession.builder()
        .addContactPoint(new InetSocketAddress(host, port))
        .withLocalDatacenter("datacenter1")
        .build();
  }

  public GenericRow executeQuery(
      final String statement,
      final Query query) {
    final String from = ((AliasedRelation) query.getFrom()).getAlias();
    final DataSource<?> dataSource = metaStore.getSource(from);
    final List<SelectItem> selectItemList = query.getSelect().getSelectItems();
    final Row row = session
        .execute(getCassandraQuery(statement, query))
        .one();

    final List<Object> result = new ArrayList<>();
    if (selectItemList.size() == 1 && selectItemList.get(0).toString().equals("*")) {
      for (Field field : dataSource.getSchema().valueSchema().fields()) {
        result.add(getResultItem(row, field));
      }
    } else {
      for (final SelectItem item : selectItemList) {
        result.add(getResultItem(row, dataSource.getSchema().findField(item.toString()).get()));
      }
    }
    return new GenericRow(result);
  }

  private String getCassandraQuery(final String statement, final Query query) {
    final String from = ((AliasedRelation) query.getFrom()).getAlias();
    String cassandraQuery =
        statement.replace(from, from + "." + metaStore.getSource(from).getKafkaTopicName());
    if (query.getWhere().isPresent()) {
      cassandraQuery = cassandraQuery.replace(";"," ALLOW FILTERING;");
    }
    return cassandraQuery;
  }

  private Object getResultItem(final Row row, final Field field) {
    switch (field.schema().type()) {
      case STRING:
        return row.getString(field.name());
      case BOOLEAN:
        return row.getBoolean(field.name());
      case INT8:
      case INT16:
      case INT32:
        return row.getInt(field.name());
      case INT64:
        return row.getLong(field.name());
      case FLOAT32:
      case FLOAT64:
        return row.getFloat(field.name());
      case MAP:
        return row.getMap(field.name(), Object.class, Object.class);
      default:
        return row.get(field.name(), Object.class);

    }
  }
}
