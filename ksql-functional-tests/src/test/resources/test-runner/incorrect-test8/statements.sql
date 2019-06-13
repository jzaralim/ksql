CREATE STREAM TEST (ID int, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='DELIMITED', key='ID');
INSERT INTO TEST VALUES (101, 'abc', 13.54);
INSERT INTO TEST VALUES (14, 'foo', 4.5);
INSERT INTO TEST2 (ID, NAME) VALUES (123, 'bar');
CREATE STREAM S1 as SELECT name, value FROM test where id > 100;