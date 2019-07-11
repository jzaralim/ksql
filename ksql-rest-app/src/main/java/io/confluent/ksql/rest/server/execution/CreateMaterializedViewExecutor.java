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

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.parser.tree.CreateMaterializedView;
import io.confluent.ksql.rest.client.KsqlConnectClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ConnectRequest;
import io.confluent.ksql.rest.entity.ConnectorEntity;
import io.confluent.ksql.rest.entity.ConnectorInfo;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;

import java.util.Optional;

public final class CreateMaterializedViewExecutor {
  private CreateMaterializedViewExecutor() {

  }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<CreateMaterializedView> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext) {
    final KsqlConfig config = statement.getConfig();
    final DataSource<?> dataSource = executionContext
        .getMetaStore()
        .getSource(statement.getStatement().getSource());
    if (!(dataSource instanceof KsqlTable)) {
      return Optional.empty();
    }
    final ConnectRequest request = new ConnectRequest(
        statement.getStatement().getMaterializedViewName(),
        (KsqlTable) dataSource,
        config);
    final KsqlConnectClient client =
        new KsqlConnectClient(
            config
            .getAllConfigPropsWithSecretsObfuscated()
            .get(KsqlConfig.CONNECT_URL_PROPERTY)
        );
    final RestResponse<ConnectorInfo> response = client.createNewConnector(request);
    return Optional.of(new ConnectorEntity(statement.getStatementText(), response.getResponse()));
  }
}
