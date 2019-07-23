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
import com.datastax.oss.driver.api.core.cql.ResultSet;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.util.MetaStoreFixture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MaterializedQueryExecutorTest {
  private MaterializedQueryExecutor executor;
  private CqlSession session = mock(CqlSession.class);
  private MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());

  @Before
  public void init() {
    final ResultSet mockRows = mock(ResultSet.class);
    when(mockRows.iterator()).thenReturn(Collections.emptyIterator());
    when(session.execute(anyString())).thenReturn(mockRows);
    executor = new MaterializedQueryExecutor(session, metaStore);
  }

  @Test
  public void shouldExecuteQuery() {
    final String sql = "SELECT * FROM MATVIEW;";
    final List<SelectItem> items = new ArrayList<>();
    items.add(mock(AllColumns.class));
    executor.executeQuery(
        sql,
        new Query(
            new Select(Optional.empty(), items),
            new AliasedRelation(mock(Relation.class), "MATVIEW"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            OptionalInt.empty()
        )
    );
    verify(session).execute("SELECT * FROM MATVIEW.test2;");
  }

  @Test
  public void shouldCapitalizeSource() {
    final String sql = "SELECT * FROM matview;";
    final List<SelectItem> items = new ArrayList<>();
    items.add(mock(AllColumns.class));
    executor.executeQuery(
        sql,
        new Query(
            new Select(Optional.empty(), items),
            new AliasedRelation(mock(Relation.class), "matview"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            OptionalInt.empty()
        )
    );
    verify(session).execute("SELECT * FROM MATVIEW.test2;");
  }

  @Test
  public void shouldExecuteQueryWithWhereClause() {
    final String sql = "SELECT * FROM MATVIEW WHERE COL0=500;";
    final List<SelectItem> items = new ArrayList<>();
    items.add(mock(AllColumns.class));
    executor.executeQuery(
        sql,
        new Query(
            new Select(Optional.empty(), items),
            new AliasedRelation(mock(Relation.class), "MATVIEW"),
            Optional.empty(),
            Optional.of(mock(Expression.class)),
            Optional.empty(),
            Optional.empty(),
            OptionalInt.empty()
        )
    );
    verify(session).execute("SELECT * FROM MATVIEW.test2 WHERE COL0=500 ALLOW FILTERING;");
  }
}