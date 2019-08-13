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


package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.rest.client.KsqlConnectClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ConnectRequest;
import io.confluent.ksql.rest.entity.ConnectorInfo;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.statement.ConfiguredStatement;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class MaterializedViewExecutorTest {
  @Rule
  public final TemporaryEngine engine = new TemporaryEngine();

  @Mock
  private final KsqlConnectClient client = new KsqlConnectClient("http://foo");

  @Before
  public void init() {
    when(client.createNewConnector(any()))
        .thenReturn(RestResponse.of(new ConnectorInfo("FOO", null, null)));
    when(client.deleteConnector(anyString()))
        .thenReturn(RestResponse.of(""));
  }

  @Test
  public void shouldCreateMaterializedView() {
    // Given:
    final ConfiguredStatement<?> createMaterializedView = engine.configure("CREATE MATERIALIZED VIEW FOO AS SELECT * FROM BAR;");
    engine.givenSource(DataSource.DataSourceType.KTABLE, "BAR");

    KsqlEngine engine = mock(KsqlEngine.class);
    when(engine.getMetaStore()).thenReturn(this.engine.getEngine().getMetaStore());

    // When:
    MaterializedViewExecutor.create(client, createMaterializedView, engine);

    // Then:
    final ArgumentCaptor<ConnectRequest> argument = ArgumentCaptor.forClass(ConnectRequest.class);
    verify(client).createNewConnector(argument.capture());
    assertThat(argument.getValue().getName(), equalTo("FOO"));
  }

  @Test
  public void shouldDropMaterializedView() {
    // Given:
    final ConfiguredStatement<?> dropMaterializedView = engine.configure("DROP MATERIALIZED FOO;");

    engine.givenSource(DataSource.DataSourceType.MATERIALIZED, "FOO");
    KsqlEngine engine = mock(KsqlEngine.class);
    when(engine.getMetaStore()).thenReturn(this.engine.getEngine().getMetaStore());

    // When:
    MaterializedViewExecutor.drop(client, dropMaterializedView, engine);

    // Then:
    verify(client).deleteConnector("FOO");
  }
}