/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.expression.tree;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.parser.NodeLocation;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

public class BytesLiteral extends Literal {

  final ByteBuffer value;

  public BytesLiteral(final ByteBuffer value) {
    this(Optional.empty(), value);
  }

  public BytesLiteral(final Optional<NodeLocation> location, final ByteBuffer value) {
    super(location);
    this.value = requireNonNull(value, "value");
  }

  @Override
  public ByteBuffer getValue() {
    return value;
  }

  @Override
  protected <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitBytesLiteral(this, context);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final BytesLiteral that = (BytesLiteral) o;
    return Objects.equals(value, that.value);
  }
}
