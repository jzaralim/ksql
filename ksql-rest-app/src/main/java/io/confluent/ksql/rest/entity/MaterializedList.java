/*
 * Copyright 2018 Confluent Inc.
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MaterializedList extends KsqlEntity {
  private final Collection<SourceInfo.Materialized> materializedViews;

  @JsonCreator
  public MaterializedList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("materializedViews") final Collection<SourceInfo.Materialized> materializedViews
  ) {
    super(statementText);
    this.materializedViews = materializedViews;
  }

  public List<SourceInfo.Materialized> getMaterializedViews() {
    return new ArrayList<>(materializedViews);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MaterializedList)) {
      return false;
    }
    final MaterializedList that = (MaterializedList) o;
    return Objects.equals(getMaterializedViews(), that.getMaterializedViews());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMaterializedViews());
  }
}
