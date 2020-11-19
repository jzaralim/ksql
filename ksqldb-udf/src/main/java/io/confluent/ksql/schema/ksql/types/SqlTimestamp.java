package io.confluent.ksql.schema.ksql.types;

import io.confluent.ksql.schema.utils.FormatOptions;

public class SqlTimestamp extends SqlType {
  public SqlTimestamp() {
    super(SqlBaseType.TIMESTAMP);
  }

  @Override
  public void validateValue(final Object value) {

  }

  public String toString(FormatOptions formatOptions) {
    return "yolo";
  }
}
