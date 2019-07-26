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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.parser.tree.CreateMaterializedView;
import io.confluent.ksql.parser.tree.DropMaterialized;
import io.confluent.ksql.parser.tree.PauseMaterialized;
import io.confluent.ksql.parser.tree.ResumeMaterialized;
import io.confluent.ksql.parser.tree.StatusMaterialized;
import io.confluent.ksql.rest.client.KsqlConnectClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ConnectRequest;
import io.confluent.ksql.rest.entity.ConnectorEntity;
import io.confluent.ksql.rest.entity.ConnectorInfo;
import io.confluent.ksql.rest.entity.ConnectorStatus;
import io.confluent.ksql.rest.entity.ConnectorStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

import java.util.Optional;

public final class MaterializedViewExecutor {
  private MaterializedViewExecutor() {

  }

  public static Optional<KsqlEntity> create(
      final ConfiguredStatement<CreateMaterializedView> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext) {
    final KsqlConfig config = statement.getConfig();
    return create(getConnectClient(config), statement, executionContext);
  }

  public static Optional<KsqlEntity> create(
      final KsqlConnectClient client,
      final ConfiguredStatement<?> statement,
      final KsqlExecutionContext executionContext) {
    final KsqlConfig config = statement.getConfig();
    final DataSource<?> dataSource = executionContext
        .getMetaStore()
        .getSource(((CreateMaterializedView) statement.getStatement()).getSource());

    verifyDatasource(
        dataSource.getName(),
        executionContext.getMetaStore(),
        DataSource.DataSourceType.KTABLE
    );

    final ConnectRequest request = new ConnectRequest(
        ((CreateMaterializedView) statement.getStatement()).getMaterializedViewName(),
        (KsqlTable<?>) dataSource,
        config
    );
    final RestResponse<ConnectorInfo> response = client.createNewConnector(request);
    return Optional.of(new ConnectorEntity(statement.getStatementText(), response.getResponse()));
  }

  public static Optional<KsqlEntity> drop(
      final ConfiguredStatement<DropMaterialized> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext) {
    final KsqlConfig config = statement.getConfig();
    return drop(getConnectClient(config), statement, executionContext);
  }

  public static Optional<KsqlEntity> drop(
      final KsqlConnectClient client,
      final ConfiguredStatement<?> statement,
      final KsqlExecutionContext executionContext) {
    final String materializedViewName =
        ((DropMaterialized) statement.getStatement()).getName().toString();
    verifyDatasource(
        materializedViewName,
        executionContext.getMetaStore(),
        DataSource.DataSourceType.MATERIALIZED
    );
    client.deleteConnector(materializedViewName);
    return Optional.empty();
  }

  public static Optional<KsqlEntity> pause(
      final ConfiguredStatement<PauseMaterialized> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext) {
    final KsqlConfig config = statement.getConfig();
    return pause(getConnectClient(config), statement, executionContext);
  }

  public static Optional<KsqlEntity> pause(
      final KsqlConnectClient client,
      final ConfiguredStatement<?> statement,
      final KsqlExecutionContext executionContext) {
    final String materializedViewName =
        ((PauseMaterialized) statement.getStatement())
        .getMaterializedViewName()
        .getSuffix();
    verifyDatasource(
        materializedViewName,
        executionContext.getMetaStore(),
        DataSource.DataSourceType.MATERIALIZED
    );
    client.pauseConnector(materializedViewName);
    return Optional.empty();
  }

  public static Optional<KsqlEntity> resume(
      final ConfiguredStatement<ResumeMaterialized> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext) {
    final KsqlConfig config = statement.getConfig();
    return resume(getConnectClient(config), statement, executionContext);
  }

  public static Optional<KsqlEntity> resume(
      final KsqlConnectClient client,
      final ConfiguredStatement<?> statement,
      final KsqlExecutionContext executionContext) {
    final String materializedViewName =
        ((ResumeMaterialized) statement.getStatement()).getMaterializedViewName().getSuffix();
    verifyDatasource(
        materializedViewName,
        executionContext.getMetaStore(),
        DataSource.DataSourceType.MATERIALIZED
    );
    client.resumeConnector(materializedViewName);
    return Optional.empty();
  }

  public static Optional<KsqlEntity> status(
      final ConfiguredStatement<StatusMaterialized> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext) {
    final KsqlConfig config = statement.getConfig();
    return status(getConnectClient(config), statement, executionContext);
  }

  public static Optional<KsqlEntity> status(
      final KsqlConnectClient client,
      final ConfiguredStatement<?> statement,
      final KsqlExecutionContext executionContext) {
    final String materializedViewName =
        ((StatusMaterialized) statement.getStatement()).getMaterializedViewName().getSuffix();
    verifyDatasource(
        materializedViewName,
        executionContext.getMetaStore(),
        DataSource.DataSourceType.MATERIALIZED
    );
    final RestResponse<ConnectorStatus> response = client.getConnectorStatus(materializedViewName);
    return Optional.of(
        new ConnectorStatusEntity(statement.getStatementText(), response.getResponse()));
  }

  @VisibleForTesting
  static KsqlConnectClient getConnectClient(final KsqlConfig config) {
    return new KsqlConnectClient(
        config
            .getAllConfigPropsWithSecretsObfuscated()
            .get(KsqlConfig.CONNECT_URL_PROPERTY)
    );
  }

  private static void verifyDatasource(
      final String dataSourceName,
      final MetaStore metaStore,
      final DataSource.DataSourceType type) {
    final DataSource<?> dataSource = metaStore.getSource(dataSourceName);

    if (dataSource == null) {
      throw new KsqlException(String.format("Unknown materialized view %s", dataSourceName));
    } else if (dataSource.getDataSourceType() != type) {
      throw new KsqlException(
          String.format(
              "Incompatible data type; got %s, expected %s",
              dataSource.getDataSourceType().getKsqlType(),
              type.getKsqlType())
      );
    }

  }
}
