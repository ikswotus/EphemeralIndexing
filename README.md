# EphemeralIndex
 
Provides a simple windows service to manage creating dynamic indexes on individual chunks of a timescale hypertable.


The main motivation for this is to save disk space by avoiding indexing the entire table, and instead only index recent data. This is mainly intended for data that will be queried in a dashboard view, and will be refreshed frequently, but always looks at now() - X hours.

TODO: image chunks

## Configuration
Uses a very simple xml file to control lifecycle of indexes on chunks.





#DEMO

First, create a test table and make it a hypertable:
```CREATE TABLE test.ephemeral_demo
(
    metric_name text COLLATE pg_catalog."default" NOT NULL,
    sample_time timestamp with time zone NOT NULL,
    current_value numeric NOT NULL
);
select create_hypertable('test.ephemeral_demo', 'sample_time');
select set_chunk_time_interval('test.ephemeral_demo', interval '1 hour');```

Setup a tablespace for the indexes: (This is mostly for tracking disk space easily...might be made configurable at some point)
```CREATE TABLESPACE eph_idx
  OWNER postgres
  LOCATION 'E:\Test\ephemera_indexes';```


Next, generate some sample data:
```insert into test.ephemeral_demo
select 'machine-1',generate_series,  floor(random()* (1000-1+ 1) + 1) * 1.0 
from generate_series(CURRENT_TIMESTAMP - interval '8 hours', CURRENT_TIMESTAMP, '15 minutes')```

```insert into test.ephemeral_demo
select 'machine-2',generate_series,  floor(random()* (1000-1+ 1) + 1) * 1.0 
from generate_series(CURRENT_TIMESTAMP - interval '8 hours', CURRENT_TIMESTAMP, '15 minutes')```

```insert into test.ephemeral_demo
select 'machine-3',generate_series,  floor(random()* (1000-1+ 1) + 1) * 1.0 
from generate_series(CURRENT_TIMESTAMP - interval '8 hours', CURRENT_TIMESTAMP, '15 minutes')```


```select show_chunks('test.ephemeral_demo');```

Output should look like: (hyper_#_## will depend on number of hypertables/chunks already created)
"_timescaledb_internal._hyper_9_16_chunk"
"_timescaledb_internal._hyper_9_17_chunk"
"_timescaledb_internal._hyper_9_18_chunk"
"_timescaledb_internal._hyper_9_19_chunk"
"_timescaledb_internal._hyper_9_20_chunk"
"_timescaledb_internal._hyper_9_21_chunk"
"_timescaledb_internal._hyper_9_22_chunk"
"_timescaledb_internal._hyper_9_23_chunk"
"_timescaledb_internal._hyper_9_24_chunk"

Inside TestApplication, we can run the 'Demo' method. This should create indexes on 3 of the tables.
Messages should be logged to the console, and we can verify by checking the db.
All indexes the service creates will be prefixed with 'ephemeral_hyper_'. The entire format is:
'ephemeral_hyper_' + [IndexName from options file] + ChunkName.
where chunknames controlled by timescaled, but should be in the form: _hyper_#_##_chunk

```
select concat('_timescaledb_internal', '.', tablename), indexname from pg_indexes where indexname like 'ephemeral_hyper_%';

```

Sample output:
| Chunk Table | Index Name|
|-------------|-----------|
|_timescaledb_internal._hyper_9_22_chunk|ephemeral_hyper_metric_time__hyper_9_22_chunk|
|_timescaledb_internal._hyper_9_23_chunk|ephemeral_hyper_metric_time__hyper_9_23_chunk|
|_timescaledb_internal._hyper_9_24_chunk|ephemeral_hyper_metric_time__hyper_9_24_chunk|


Running Demo() again shouldn't change any of the indexing (unless the hour rolls over...)

Demo(1) should shorten our age index, and result in several indexes being dropped.
Running our query again should only show a single table now:

```
select concat('_timescaledb_internal', '.', tablename), indexname from pg_indexes where indexname like 'ephemeral_hyper_%';

```

| Chunk Table | Index Name|
|-------------|-----------|
|_timescaledb_internal._hyper_9_24_chunk|ephemeral_hyper_metric_time__hyper_9_24_chunk|

