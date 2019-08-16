# Materialized Views
This is an proof of concept of a possible implementation of interactive queries. It works by connecting tables to a Cassandra database and forwarding queries to Cassandra.

## Setup
1. Download and run [Cassandra](http://cassandra.apache.org/download/).
2. Clone into the [KeyToValue SMT](https://github.com/jzaralim/KeyToValueTransform) and build it. Add the path to the generated `.jar` file to the connect configs.
3. Start up a Kafka Connect cluster.
4. Add the Cassandra and Connect endpoints to `ksql-server.properties`
```
# The URL of the Connect cluster
ksql.connect.url=http://localhost:8083

# The URL of the Cassandra database
ksql.cassandra.host=localhost
ksql.cassandra.port=9042
```
4. Run KSQL!

## Commands
The following examples use the `pageviews` stream and `users` table from the datagen connector.

### CREATE
To materialize a view, the command `CREATE MATERIALIZED VIEW <name> AS SELECT * FROM <table>;` is used. Currently, only `SELECT * FROM <table>` queries are accepted.

Create materialized views for non-windowed and windowed tables.
```
CREATE MATERIALIZED VIEW FOO AS SELECT * FROM USERS;
```
```
CREATE MATERIALIZED VIEW BAR AS SELECT * FROM PAGEVIEWS_REGIONS;
```

After executing each statement, you should see the following message:

```
 Message
-----------------------------
 Materialized view  is ready
-----------------------------
```

### SHOW
Show all materialized views by running the command `SHOW MATERIALIZED;` The response should be:
```
 Materialized View Name | Kafka Topic       | Format | Windowed
----------------------------------------------------------------
 BAR                    | PAGEVIEWS_REGIONS | AVRO   | true
 FOO                    | users             | AVRO   | false
----------------------------------------------------------------
```

### SELECT
Querying uses the same syntax as querying a KSQL table or stream. `SELECT * FROM FOO;` returns:
```
+-----------------------------------------+-----------------------------------------+-----------------------------------------+-----------------------------------------+
|REGISTERTIME                             |USERID                                   |REGIONID                                 |GENDER                                   |
+-----------------------------------------+-----------------------------------------+-----------------------------------------+-----------------------------------------+
|1517748905020                            |User_3                                   |Region_8                                 |MALE                                     |
|1512722291296                            |User_1                                   |Region_6                                 |OTHER                                    |
|1515320469101                            |User_6                                   |Region_3                                 |OTHER                                    |
|1492978435928                            |User_7                                   |Region_2                                 |FEMALE                                   |
|1488289647818                            |User_2                                   |Region_6                                 |OTHER                                    |
|1504970411843                            |User_4                                   |Region_8                                 |OTHER                                    |
|1496311855652                            |User_5                                   |Region_6                                 |MALE                                     |
|1504189885452                            |User_8                                   |Region_5                                 |FEMALE                                   |
|1491010434870                            |User_9                                   |Region_4                                 |MALE                                     |
```

Other simple clauses can be used as well. For example, `SELECT USERID, REGIONID from FOO WHERE GENDER='MALE';` returns:
```
+-------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------+
|USERID                                                                               |REGIONID                                                                             |
+-------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------+
|User_5                                                                               |Region_9                                                                             |
|User_9                                                                               |Region_6                                                                             |
```

Querying a windowed table returns the Windowed Key along with the rest of the columns. `SELECT * from BAR LIMIT 3;` returns:
```
+-----------------------------------------+-----------------------------------------+-----------------------------------------+-----------------------------------------+
|GENDER                                   |REGIONID                                 |NUMUSERS                                 |WindowedKey                              |
+-----------------------------------------+-----------------------------------------+-----------------------------------------+-----------------------------------------+
|FEMALE                                   |Region_8                                 |31                                       |FEMALE|+|Region_8 : Window{start=15639962|
|                                         |                                         |                                         |10000 end=1563996240000}                 |
|FEMALE                                   |Region_3                                 |27                                       |FEMALE|+|Region_3 : Window{start=102499|
|                                         |                                         |                                         |702678565440 end=102499702678595440}     |
|FEMALE                                   |Region_9                                 |55                                       |FEMALE|+|Region_9 : Window{start=102498|
|                                         |                                         |                                         |913210055952 end=102498913210085952}     |
```

### PAUSE
If it was running, pausing materialization stops KSQL from updating the materialized view, leaving it in the state that it was in when PAUSE was called.

Pause by running the command `PAUSE MATERIALIZED FOO;`. Run `SELECT * FROM FOO;`, and again after 10 seconds. The results should be identical.

### RESUME
If it was paused, resuming will start updating the materialized view again. Resume by running `RESUME MATERIALIZED FOO;`. Run `SELECT * FROM FOO;`. It should be different from the previous results.

### DROP
Drop the materialized table by running `DROP MATERIALIZED FOO;`. You should get the following response:
```
 Message
----------------------------------------
 Source FOO (topic: users) was dropped.
----------------------------------------
```
This command only drops the materialization and not the underlying table.

## Windowed Keys
Windowed key serialization/deserialization is broken. The rowkey always gets extracted correctly, but the window times
are inaccurate. I investigated it and that the key in the topic and the key in Cassandra are different. I don't know
how or why that happens to windowed keys but not other types of keys though. A workaround could be to add some string
transformations to the KeyToValue SMT.