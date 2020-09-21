

# Current situation

## Typical stack

- Database: Postgresql / Oracle / Cassandra
- Backend: Python / C# / java
- Protocol: REST (JSON / XML)


## Challenges

- Large queries (data streaming)
- Authentication
- Backups
- Caching / Change detection
- Scaling


# Lakota

Lakota is a versionned columnar storage.


## Inspirations

- Zarr: chunking and compression of numerical arrays
- Git: Versioning


## Quickstart


New timeseries:

    bch@wsl:/tmp/data-dir$ lakota create temp/bxl "timestamp timestamp*" "value float"
	bch@wsl:/tmp/data-dir$ tree -h
	.
	├── [4.0K]  00
	│   └── [4.0K]  00
	│       └── [4.0K]  000000000000000000000000000000000000
	│           └── [ 163]  0000000000000000000000000000000000000000.174b1e42f22-144cd6c8fa1867014cfa3027437121b9d31df98c
	├── [4.0K]  37
	│   └── [4.0K]  c7
	│       └── [  29]  32cfaa6bb6218a10a65a9013bcc4e8f95a4c
	├── [4.0K]  75
	│   └── [4.0K]  7f
	│       └── [  65]  fb5073f8e9510b51209b9218d6f8c9d2b782
	├── [4.0K]  7d
	│   └── [4.0K]  9d
	│       └── [  98]  f5a6e1055d45c771d5fe5f0561aba146b597
	├── [4.0K]  a0
	│   └── [4.0K]  11
	│       └── [  30]  ebd7c7799e993d62d6c2a6370b485b8385ec
	├── [4.0K]  d9
	│   └── [4.0K]  69
	│       └── [4.0K]  831eb8a99cff8c02e681f43289e5d3d69664
	│           └── [ 160]  0000000000000000000000000000000000000000.174b1e42f2f-914eecb7f5cf1dea29719e10e84f481613c16206
	├── [  96]  input-corrected.csv
	└── [  84]  input.csv

	14 directories, 8 files

Some content:

	bch@wsl:/tmp/data-dir$ cat input.csv
	2020-06-22,25
	2020-06-23,24
	2020-06-24,27
	2020-06-25,31
	2020-06-26,32
	2020-06-27,30


First write:

	bch@wsl:/tmp/data-dir$ cat input.csv | lakota write temp/bxl
	bch@wsl:/tmp/data-dir$ tree -h
	.
	├── [4.0K]  00
	│   └── [4.0K]  00
	│       └── [4.0K]  000000000000000000000000000000000000
	│           └── [ 163]  0000000000000000000000000000000000000000.174b1e42f22-144cd6c8fa1867014cfa3027437121b9d31df98c
	├── [4.0K]  37
	│   └── [4.0K]  c7
	│       └── [  29]  32cfaa6bb6218a10a65a9013bcc4e8f95a4c
	├── [4.0K]  5e
	│   └── [4.0K]  66
	│       └── [  64]  a21b02e8fca62643f45dcb5e1b9c4e99edd9
	├── [4.0K]  75
	│   └── [4.0K]  7f
	│       └── [  65]  fb5073f8e9510b51209b9218d6f8c9d2b782
	├── [4.0K]  7d
	│   └── [4.0K]  9d
	│       └── [  98]  f5a6e1055d45c771d5fe5f0561aba146b597
	├── [4.0K]  a0
	│   └── [4.0K]  11
	│       └── [  30]  ebd7c7799e993d62d6c2a6370b485b8385ec
	├── [4.0K]  ce
	│   └── [4.0K]  b1
	│       └── [  64]  c30243befa63f147509f83e0458cc906e69e
	├── [4.0K]  d9
	│   └── [4.0K]  69
	│       └── [4.0K]  831eb8a99cff8c02e681f43289e5d3d69664
	│           ├── [ 160]  0000000000000000000000000000000000000000.174b1e42f2f-914eecb7f5cf1dea29719e10e84f481613c16206
	│           └── [ 169]  174b1e42f2f-914eecb7f5cf1dea29719e10e84f481613c16206.174b1e767fa-554d6f363512625267676f871b675038c772ef4c
	├── [  96]  input-corrected.csv
	└── [  84]  input.csv

	18 directories, 11 files

Updated content:

	bch@wsl:/tmp/data-dir$ cat input-corrected.csv
	2020-06-22,25.2
	2020-06-23,24.2
	2020-06-24,27.9
	2020-06-25,31.0
	2020-06-26,32.5
	2020-06-27,30.1


Second write:

    bch@wsl:/tmp/data-dir$ cat input-corrected.csv | lakota write temp/bxl
    bch@wsl:/tmp/data-dir$ tree -h
    .
    ├── [4.0K]  00
    │   └── [4.0K]  00
    │       └── [4.0K]  000000000000000000000000000000000000
    │           └── [ 163]  0000000000000000000000000000000000000000.174b1e42f22-144cd6c8fa1867014cfa3027437121b9d31df98c
    ├── [4.0K]  37
    │   └── [4.0K]  c7
    │       └── [  29]  32cfaa6bb6218a10a65a9013bcc4e8f95a4c
    ├── [4.0K]  5e
    │   └── [4.0K]  66
    │       └── [  64]  a21b02e8fca62643f45dcb5e1b9c4e99edd9
    ├── [4.0K]  75
    │   └── [4.0K]  7f
    │       └── [  65]  fb5073f8e9510b51209b9218d6f8c9d2b782
    ├── [4.0K]  7d
    │   └── [4.0K]  9d
    │       └── [  98]  f5a6e1055d45c771d5fe5f0561aba146b597
    ├── [4.0K]  a0
    │   └── [4.0K]  11
    │       └── [  30]  ebd7c7799e993d62d6c2a6370b485b8385ec
    ├── [4.0K]  ce
    │   └── [4.0K]  b1
    │       └── [  64]  c30243befa63f147509f83e0458cc906e69e
    ├── [4.0K]  d9
    │   └── [4.0K]  69
    │       └── [4.0K]  831eb8a99cff8c02e681f43289e5d3d69664
    │           ├── [ 160]  0000000000000000000000000000000000000000.174b1e42f2f-914eecb7f5cf1dea29719e10e84f481613c16206
    │           ├── [ 169]  174b1e42f2f-914eecb7f5cf1dea29719e10e84f481613c16206.174b1e767fa-554d6f363512625267676f871b675038c772ef4c
    │           └── [ 168]  174b1e767fa-554d6f363512625267676f871b675038c772ef4c.174b1e8514c-cf1e8cb0d4ba757b80b027ef6efb7e05c3f69e77
    ├── [4.0K]  f7
    │   └── [4.0K]  c5
    │       └── [  64]  9ab6f25820e9eb0cb72fa2219f9c643d44b5
    ├── [  96]  input-corrected.csv
    └── [  84]  input.csv

	20 directories, 13 files



Read back the result:

	bch@wsl:/tmp/data-dir$ lakota read temp/bxl
	timestamp              value
	-------------------  -------
	2020-06-22T00:00:00     25
	2020-06-22T00:00:00     25.2
	2020-06-23T00:00:00     24.2
	2020-06-24T00:00:00     27.9
	2020-06-25T00:00:00     31
	2020-06-26T00:00:00     32.5
	2020-06-27T00:00:00     30.1


Pack series:

	bch@wsl:/tmp/data-dir$ lakota pack temp
	bch@wsl:/tmp/data-dir$ tree -h
	.
	├── [4.0K]  00
	│   └── [4.0K]  00
	│       └── [4.0K]  000000000000000000000000000000000000
	│           └── [ 163]  0000000000000000000000000000000000000000.174b1e42f22-144cd6c8fa1867014cfa3027437121b9d31df98c
	├── [4.0K]  37
	│   └── [4.0K]  c7
	│       └── [  29]  32cfaa6bb6218a10a65a9013bcc4e8f95a4c
	├── [4.0K]  5e
	│   └── [4.0K]  66
	│       └── [  64]  a21b02e8fca62643f45dcb5e1b9c4e99edd9
	├── [4.0K]  75
	│   └── [4.0K]  7f
	│       └── [  65]  fb5073f8e9510b51209b9218d6f8c9d2b782
	├── [4.0K]  7d
	│   └── [4.0K]  9d
	│       └── [  98]  f5a6e1055d45c771d5fe5f0561aba146b597
	├── [4.0K]  a0
	│   └── [4.0K]  11
	│       └── [  30]  ebd7c7799e993d62d6c2a6370b485b8385ec
	├── [4.0K]  ce
	│   └── [4.0K]  b1
	│       └── [  64]  c30243befa63f147509f83e0458cc906e69e
	├── [4.0K]  d9
	│   └── [4.0K]  69
	│       └── [4.0K]  831eb8a99cff8c02e681f43289e5d3d69664
	│           └── [ 311]  0000000000000000000000000000000000000000.174b1ed49eb-e98af129d75cc3584d1417f92f432d4da61e15f6
	├── [4.0K]  f7
	│   └── [4.0K]  c5
	│       └── [  64]  9ab6f25820e9eb0cb72fa2219f9c643d44b5
	├── [  96]  input-corrected.csv
	└── [  84]  input.csv

	20 directories, 11 files

Garbage collection & squash registry series

    TODO
