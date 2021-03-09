

# Context

The problem Lakota is trying to solve is the storage of large
numerical series in general and timeseries in particular:

- Write once, read many pattern
- Most users do frequent refresh of a small group of series (but
  different users will work on different groups of series)
- Usually access a short horizon (the most recent data) for example to
  compute prediction, but sometimes will re-read a large history (for
  model calibration)


## DBMS

In-house solutions based on relational databases often comes with an
applicative layer (aka "backend") that while providing custom features
is also a chokepoint:

- Data-streaming over a REST api is not easy to implement (something
  databases and database driver supports). So, if not forbidden, large
  queries will at best jeopardize the performances and a worst will
  result in a deny of service of the application server itself.
- No easy way to detect changes or to consult history of changes
- Scaling over several servers is expensive
- The applicative layer has to (re-)implement ancillary features like
  user management and access rights, usage statistics, protection
  against DOS, etc.
- When the dataset stored in DB gets large, backup management and so
  uptime garantee quickly becomes non-trivial.


## S3

S3 (and S3 clones) strong points:

- High durability garantee
- Scale horizontally
- Handle massive datasets
- Simple and ubiquitous API supported by different solutions and providers
- Easy to mirror and proxy
- Extensive ecosystem 

But:

- No concurrency control
- No history of changes
- No indexing beside file path


## Zarr

Beside Git, [Zarr](zarr.readthedocs.io/) is another important
influence of Lakota. It offers chunked storage of multi-dimensional
arrays and a broad choice of storage backend, including S3. But it
lacks the ability to change an existing array, it can only append to
it. There is also no builtin concurrency control.


# Lakota:

Lakota organise reads and writes through a changelog inspired by Git
(and by DVCSs in general). This changelog provides: historization,
concurrency control and ease of synchronisation across different
storages.

- Supports S3 but also local filesystem and in-memory storage
- Compress numerical arrays thanks to [numcodecs](https://numcodecs.readthedocs.io)
- The lakota changelog provides both versioning and indexing
- Ability to edit an existing array without full rewrite

