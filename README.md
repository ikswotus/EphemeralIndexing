# EphemeralIndex
 
Provides a simple windows service to manage creating dynamic indexes on individual chunks of a timescale hypertable.


The main motivation for this is to save disk space by avoiding indexing the entire table, and instead only index recent data. This is mainly intended for data that will be queried in a dashboard view, and will be refreshed frequently, but always looks at now() - X hours.

TODO: image chunks

## Configuration
Uses a very simple xml file to control lifecycle of indexes on chunks.


