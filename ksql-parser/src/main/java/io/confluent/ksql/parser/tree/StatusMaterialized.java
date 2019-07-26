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

package io.confluent.ksql.parser.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;

import java.util.Objects;
import java.util.Optional;

@Immutable
public class StatusMaterialized extends Statement {
  private final QualifiedName materializedViewName;

  public StatusMaterialized(final QualifiedName materializedViewName) {
    this(Optional.empty(), materializedViewName);
  }

  public StatusMaterialized(
      final Optional<NodeLocation> location,
      final QualifiedName materializedViewName) {
    super(location);
    this.materializedViewName =
        requireNonNull(materializedViewName, "materializedViewName is null");
  }

  public QualifiedName getMaterializedViewName() {
    return materializedViewName;
  }

  @Override
  public int hashCode() {
    return Objects.hash(materializedViewName);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final StatusMaterialized o = (StatusMaterialized) obj;
    return Objects.equals(materializedViewName, o.materializedViewName);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("materializedViewName", materializedViewName)
        .toString();
  }
}
