package io.confluent.ksql.function.types;

public class TimestampType extends ObjectType {
  public static final TimestampType INSTANCE = new TimestampType();

  private TimestampType() {
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }

  @Override
  public String toString() {
    return null;
  }
}
