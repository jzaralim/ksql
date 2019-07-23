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

package io.confluent.ksql.parser.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;

public class CreateMaterializedView extends Statement {
  private final String materializedViewName;
  private final String source;

  public CreateMaterializedView(final String materializedViewName, final String source) {
    this(Optional.empty(), materializedViewName, source);
  }

  public CreateMaterializedView(
      final Optional<NodeLocation> location,
      final String materializedViewName,
      final String source) {
    super(location);
    this.materializedViewName =
        requireNonNull(materializedViewName, "materializedViewName is null");
    this.source = requireNonNull(source, "source is null");
  }

  public String getMaterializedViewName() {
    return materializedViewName;
  }

  public String getSource() {
    return source;
  }

  @Override
  public int hashCode() {
    return Objects.hash(materializedViewName, source);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final CreateMaterializedView o = (CreateMaterializedView) obj;
    return Objects.equals(materializedViewName, o.materializedViewName)
        && Objects.equals(source, o.source);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("materializedViewName", materializedViewName)
        .add("source", source)
        .toString();
  }
}
