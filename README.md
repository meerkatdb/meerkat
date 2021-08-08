## MeerkatDB

MeerkatDB is a distributed append-only (no UPDATE/DELETE support) eventual consistent columnar storage for events and timeseries.


### Architecture overview.

MeerkatDB architecture is highly inspired by the original DynamoDB paper. MeerkatDB clusters are masterless, all nodes in the cluster are symmetric, they can act as a query coordinator, query executor, or as an ingestion node. Ingested data is partitioned in a fixed amount of partitions (shards) using consistency hashing and those shards are assigned to cluster nodes. The same shard can be allocated to a different node to achieve data replication. Data distributed among shards are eventually consistent, an anti-entropy mechanism keeps the data consistency between nodes. Metadata about databases, tables. partition allocation and partition status is disseminated infectiously using a gossip protocol.


#### Logical data model.

MeerkatDB arranges data in tables. tables are compounds of typed columns similar to SQL tables. Table schema can be enforced at ingestion time or can be created dynamically inferring the column types from the ingested data. Although the name of the tablas and columns are immutable, they can be aliased at query time.
Schemas can evolve over time even in backward-incompatible ways (ie columns having two different data types over time ). The effective schema of a table will be constructed querying the schema of all table segments for each table partition.
MeerkatDB doesn't support primary keys. A Timestamp column (_ts) is mandatory for ingested rows and for deduplication purposes a i32 dedup column (_dedup) must be provided. Row deduplication is performed when segments are merged.


#### Data ingestion

Any node in the cluster can be used to ingest data. Ingestion will be primarily schemaless, data types will be inferred at ingestion time, later we will add schema enforcing at ingestion time.  
Ingested data is buffered in memory until some configurable threshold is reached ( usually based on memory consumption or time interval ) then it is indexed and flushed to disk.
For performance reasons MeerkatDB doesn't enforce any unique or primary key constraint, however, given that clients are allowed to resubmit data in the event of a network failure, a deduplication mechanism based on an extra dedup column is used to remove duplicated rows.


### Storage

MeerkatDB storage is a LSM tree. The segments format is similar to Apache Parquet but simpler ( there are not column groups or dremel encoding ). Columns are indexed and compressed in pages and written sequentially back to back. Segments are partitioned by time ( similar to apache druid ) and merged regularly.

#### Antientropy

The anti-entropy mechanism assumes the typical case where events are ingested with a low dispersion on its timestamps. Events are partitioned by time and then a hash is calculated for each bucket using its timestamp and _dedup column. Replicas keep a logical clock for each peer in their partition and pull hashes only for recently updated buckets.

#### Query executor.
We plan to use KQL ( Kusto Query Language ) as the main quey language in MeerkatDB. Queries will be executed in a distributed vectorized query engine using Apache Arrow for memory representation and computation. Any cluster node can act as a query coordinator. Query coordinators will parse the KQL query and generate a logical plan. logical plans will be then optimized and transformed into a distributed logical plan which will be broadcasted to the nodes. The distributed plan will be locally optimized using stats from the involved segments and transformed into a physical plan to be executed later. 

