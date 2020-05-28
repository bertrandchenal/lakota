

# Context

S3 (and s3-like) strong points:

- Stores files
- Scale horizontally
- Handle massive datasets
- Simple and ubiquitous API supported by different solutions and providers
- Easy to mirror
- Extensive ecosystem 

But:

- Stores files
- No concurrency control
- No history of changes
- No indexing beside file path

Enter Zarr:

- Implements chunked and compressed arrays
- Supports S3 but also in-memory or in-database storages and local
  filesystem
- Provide efficient slicing of large arrays
- Able to append to an existing array without full rewrite
- Implemented in Python. Compatible implementations are available in
  Java, C++, Rust and Javascript.


# Baltic

Baltic organise reads and writes through a changelog inspired by Git
(and by DVCSs in general). This changelog provides: historisation,
concurrency control and ease of synchronisation across different
storages.
