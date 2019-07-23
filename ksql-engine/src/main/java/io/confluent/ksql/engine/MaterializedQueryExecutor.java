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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.MaterializedView;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.streams.kstream.Windowed;

public class MaterializedQueryExecutor {
  private final CqlSession session;
  private final MetaStore metaStore;

  public MaterializedQueryExecutor(final String host, final int port, final MetaStore metaStore) {
    this(CqlSession
        .builder()
        .addContactPoint(new InetSocketAddress(host, port))
        .withLocalDatacenter("datacenter1")
        .build(), metaStore);
  }

  @VisibleForTesting
  MaterializedQueryExecutor(final CqlSession session, final MetaStore metaStore) {
    this.session = session;
    this.metaStore = metaStore;
  }

  public List<GenericRow> executeQuery(
      final String statement,
      final Query query) {
    final String from = ((AliasedRelation) query.getFrom()).getAlias();
    final DataSource<?> dataSource = metaStore.getSource(from);
    final List<SelectItem> selectItemList = query.getSelect().getSelectItems();
    final ResultSet rows = session
        .execute(getCassandraQuery(statement, query));

    final List<GenericRow> results = new ArrayList<>();

    for (final Row row : rows) {
      final List<Object> columns = new ArrayList<>();
      for (final SelectItem item : selectItemList) {
        if (item instanceof SingleColumn) {
          columns.add(getResultItem(
              row,
              dataSource.getSchema().findField(((SingleColumn) item).getAlias()).get()
          ));
        } else {
          for (Field field : dataSource.getSchema().valueSchema().fields()) {
            columns.add(getResultItem(row, field));
          }
        }
      }
      if (((MaterializedView) dataSource).isWindowed()) {
        try {
          final Windowed windowedKey = (Windowed) dataSource
              .getKeySerdeFactory()
              .create()
              .deserializer()
              .deserialize(dataSource.getKafkaTopicName(), row.getString("rowkey").getBytes(UTF_8));
          columns.add(windowedKey.window().startTime().getEpochSecond());
          columns.add(windowedKey.window().endTime().getEpochSecond());
        } catch (Exception e) {
          columns.add(0);
          columns.add(0);
        }
      }
      results.add(new GenericRow(columns));
    }

    return results;
  }

  private String getCassandraQuery(final String statement, final Query query) {
    final String from = ((AliasedRelation) query.getFrom()).getAlias();
    final String preWhere;
    final String postWhere;

    if (query.getWhere().isPresent()) {
      final int whereIndex = statement.toUpperCase().indexOf("WHERE");
      preWhere = statement.substring(0, whereIndex).toUpperCase();
      postWhere = statement.substring(whereIndex).replace(";", " ALLOW FILTERING;");
    } else {
      preWhere = statement.toUpperCase();
      postWhere = "";
    }
    return preWhere.replace(from, from + "." + metaStore.getSource(from).getKafkaTopicName())
        + postWhere;
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
