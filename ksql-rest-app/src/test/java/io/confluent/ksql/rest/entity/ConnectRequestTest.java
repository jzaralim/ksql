package io.confluent.ksql.rest.entity;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.avro.KsqlAvroSerdeFactory;
import io.confluent.ksql.serde.delimited.KsqlDelimitedSerdeFactory;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class ConnectRequestTest {
  public static final LogicalSchema SCHEMA = LogicalSchema.of(SchemaBuilder.struct()
      .field("val", Schema.OPTIONAL_STRING_SCHEMA)
      .build());

  @Test
  public void testMakeConfigsForJson() {
    // When:
    final ConnectRequest request = new ConnectRequest(
        "foo",
        new KsqlTable<>(
            "",
            "bar",
            SCHEMA,
            SerdeOption.none(),
            KeyField.of("val", SCHEMA.valueSchema().field("val")),
            new MetadataTimestampExtractionPolicy(),
            new KsqlTopic("BAR", "bar", new KsqlJsonSerdeFactory(), false),
            Serdes::String
        ),
        KsqlConfigTestUtil.create("localhost:9092")
    );

    // Then:
    assertThat(request.getConfig().get("name"), equalTo("foo"));
    assertThat(request.getConfig().get("cassandra.keyspace"), equalTo("foo"));
    assertThat(request.getConfig().get("topics"), equalTo("bar"));
    assertThat(request.getConfig().get("value.converter"), equalTo("org.apache.kafka.connect.json.JsonConverter"));
    assertThat(request.getConfig().get("value.converter.schemas.enable"), equalTo("false"));
  }

  @Test
  public void testMakeConfigsForAvro() {
    // When:
    final ConnectRequest request = new ConnectRequest(
        "foo",
        new KsqlTable<>(
            "",
            "bar",
            SCHEMA,
            SerdeOption.none(),
            KeyField.of("val", SCHEMA.valueSchema().field("val")),
            new MetadataTimestampExtractionPolicy(),
            new KsqlTopic("BAR", "bar", new KsqlAvroSerdeFactory("schema"), false),
            Serdes::String
        ),
        KsqlConfigTestUtil.create("localhost:9092")
    );

    // Then:
    assertThat(request.getConfig().get("name"), equalTo("foo"));
    assertThat(request.getConfig().get("cassandra.keyspace"), equalTo("foo"));
    assertThat(request.getConfig().get("topics"), equalTo("bar"));
    assertThat(request.getConfig().get("value.converter"), equalTo("io.confluent.connect.avro.AvroConverter"));
    assertThat(request.getConfig().get("value.converter.schema.registry.url"), equalTo("http://localhost:8081"));
  }

  @Test
  public void testMakeConfigsForDelimited() {
    // When:
    final ConnectRequest request = new ConnectRequest(
        "foo",
        new KsqlTable<>(
            "",
            "bar",
            SCHEMA,
            SerdeOption.none(),
            KeyField.of("val", SCHEMA.valueSchema().field("val")),
            new MetadataTimestampExtractionPolicy(),
            new KsqlTopic("BAR", "bar", new KsqlDelimitedSerdeFactory(), false),
            Serdes::String
        ),
        KsqlConfigTestUtil.create("localhost:9092")
    );

    // Then:
    assertThat(request.getConfig().get("name"), equalTo("foo"));
    assertThat(request.getConfig().get("cassandra.keyspace"), equalTo("foo"));
    assertThat(request.getConfig().get("topics"), equalTo("bar"));
    assertThat(request.getConfig().get("value.converter"), equalTo("org.apache.kafka.connect.storage.StringConverter"));
  }
}