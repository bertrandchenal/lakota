

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


# Jensen

Jensen is a versionned columnar storage.


## Inspirations

- Zarr: chunking and compression of numerical arrays
- Git: Versioning


## Quickstart


New timeseries:

	bch@wsl:/tmp/data-dir$ jensen create temp_bxl "timestamp timestamp*" "value float"
    bch@wsl:/tmp/data-dir$ tree -h
    .
    ├── [  96]  input-corrected.csv
    ├── [  84]  input.csv
    ├── [4.0K]  registry
    │   └── [4.0K]  changelog
    │       └── [ 149]  0000000000000000000000000000000000000000.17449d7465c-affb0de0066806d705bbe7b6a60612d2ad4920de
    └── [4.0K]  segment
        ├── [4.0K]  2c
        │   └── [4.0K]  ff
        │       └── [  97]  b86c25d68cb19754483fb615270665e626e6
        └── [4.0K]  d5
            └── [4.0K]  8b
                └── [  34]  a9d1941bf359d86359ef27eccc9e21c4c178

    7 directories, 5 files

Some content:

	bch@wsl:/tmp/data-dir$ cat input.csv
	2020-06-22,25
	2020-06-23,24
	2020-06-24,27
	2020-06-25,31
	2020-06-26,32
	2020-06-27,30


First write:

	bch@wsl:/tmp/data-dir$ cat input.csv | jensen write temp_bxl
	bch@wsl:/tmp/data-dir$ tree -h
	.
	├── [  96]  input-corrected.csv
	├── [  84]  input.csv
	├── [4.0K]  registry
	│   └── [4.0K]  changelog
	│       └── [ 149]  0000000000000000000000000000000000000000.17449d7465c-affb0de0066806d705bbe7b6a60612d2ad4920de
	├── [4.0K]  segment
	│   ├── [4.0K]  2c
	│   │   └── [4.0K]  ff
	│   │       └── [  97]  b86c25d68cb19754483fb615270665e626e6
	│   ├── [4.0K]  5e
	│   │   └── [4.0K]  66
	│   │       └── [  64]  a21b02e8fca62643f45dcb5e1b9c4e99edd9
	│   ├── [4.0K]  ce
	│   │   └── [4.0K]  b1
	│   │       └── [  64]  c30243befa63f147509f83e0458cc906e69e
	│   └── [4.0K]  d5
	│       └── [4.0K]  8b
	│           └── [  34]  a9d1941bf359d86359ef27eccc9e21c4c178
	└── [4.0K]  series
		└── [4.0K]  0b
			└── [4.0K]  b6
				└── [4.0K]  f0edadfbce09627ee4d8d9e48c153e70236b
					└── [4.0K]  changelog
						└── [ 161]  0000000000000000000000000000000000000000.17449d8b3dc-e44d02407cf6d8a06cb5ec566338b2d6123f7f85

	16 directories, 8 files

Updated content:

	bch@wsl:/tmp/data-dir$ cat input-corrected.csv
	2020-06-22,25.2
	2020-06-23,24.2
	2020-06-24,27.9
	2020-06-25,31.0
	2020-06-26,32.5
	2020-06-27,30.1


Second write:

	bch@wsl:/tmp/data-dir$ cat input-corrected.csv | jensen write temp_bxl
	bch@wsl:/tmp/data-dir$ tree -h
	.
	├── [  96]  input-corrected.csv
	├── [  84]  input.csv
	├── [4.0K]  registry
	│   └── [4.0K]  changelog
	│       └── [ 149]  0000000000000000000000000000000000000000.17449d7465c-affb0de0066806d705bbe7b6a60612d2ad4920de
	├── [4.0K]  segment
	│   ├── [4.0K]  2c
	│   │   └── [4.0K]  ff
	│   │       └── [  97]  b86c25d68cb19754483fb615270665e626e6
	│   ├── [4.0K]  5e
	│   │   └── [4.0K]  66
	│   │       └── [  64]  a21b02e8fca62643f45dcb5e1b9c4e99edd9
	│   ├── [4.0K]  ce
	│   │   └── [4.0K]  b1
	│   │       └── [  64]  c30243befa63f147509f83e0458cc906e69e
	│   ├── [4.0K]  d5
	│   │   └── [4.0K]  8b
	│   │       └── [  34]  a9d1941bf359d86359ef27eccc9e21c4c178
	│   └── [4.0K]  f7
	│       └── [4.0K]  c5
	│           └── [  64]  9ab6f25820e9eb0cb72fa2219f9c643d44b5
	└── [4.0K]  series
		└── [4.0K]  0b
			└── [4.0K]  b6
				└── [4.0K]  f0edadfbce09627ee4d8d9e48c153e70236b
					└── [4.0K]  changelog
						├── [ 161]  0000000000000000000000000000000000000000.17449d8b3dc-e44d02407cf6d8a06cb5ec566338b2d6123f7f85
						└── [ 159]  17449d8b3dc-e44d02407cf6d8a06cb5ec566338b2d6123f7f85.17449d9b6aa-640d62eba17b9b60479b30f7c71c976937a24f4b

	18 directories, 10 files


Read back the result:

	bch@wsl:/tmp/data-dir$ jensen read temp_bxl
	timestamp              value
	-------------------  -------
	2020-06-22T00:00:00     25
	2020-06-22T00:00:00     25.2
	2020-06-23T00:00:00     24.2
	2020-06-24T00:00:00     27.9
	2020-06-25T00:00:00     31
	2020-06-26T00:00:00     32.5
	2020-06-27T00:00:00     30.1


Squash series:

	bch@wsl:/tmp/data-dir$ jensen squash temp_bxl
	bch@wsl:/tmp/data-dir$ tree -h
	.
	├── [  96]  input-corrected.csv
	├── [  84]  input.csv
	├── [4.0K]  registry
	│   └── [4.0K]  changelog
	│       └── [ 149]  0000000000000000000000000000000000000000.17449d7465c-affb0de0066806d705bbe7b6a60612d2ad4920de
	├── [4.0K]  segment
	│   ├── [4.0K]  2c
	│   │   └── [4.0K]  ff
	│   │       └── [  97]  b86c25d68cb19754483fb615270665e626e6
	│   ├── [4.0K]  5e
	│   │   └── [4.0K]  66
	│   │       └── [  64]  a21b02e8fca62643f45dcb5e1b9c4e99edd9
	│   ├── [4.0K]  ce
	│   │   └── [4.0K]  b1
	│   │       └── [  64]  c30243befa63f147509f83e0458cc906e69e
	│   ├── [4.0K]  d5
	│   │   └── [4.0K]  8b
	│   │       └── [  34]  a9d1941bf359d86359ef27eccc9e21c4c178
	│   └── [4.0K]  f7
	│       └── [4.0K]  c5
	│           └── [  64]  9ab6f25820e9eb0cb72fa2219f9c643d44b5
	└── [4.0K]  series
		└── [4.0K]  0b
			└── [4.0K]  b6
				└── [4.0K]  f0edadfbce09627ee4d8d9e48c153e70236b
					└── [4.0K]  changelog
						└── [ 159]  0000000000000000000000000000000000000000.17449da9cde-9afa8506207c6a97f83441949f020abf89505fb7

	18 directories, 9 files


Garbage collection & squash registry series

	bch@wsl:/tmp/data-dir$ jensen gc
	1 frames deleted
	bch@wsl:/tmp/data-dir$ tree -h
	.
	├── [  96]  input-corrected.csv
	├── [  84]  input.csv
	├── [4.0K]  registry
	│   └── [4.0K]  changelog
	│       └── [ 149]  0000000000000000000000000000000000000000.17449d7465c-affb0de0066806d705bbe7b6a60612d2ad4920de
	├── [4.0K]  segment
	│   ├── [4.0K]  2c
	│   │   └── [4.0K]  ff
	│   │       └── [  97]  b86c25d68cb19754483fb615270665e626e6
	│   ├── [4.0K]  5e
	│   │   └── [4.0K]  66
	│   │       └── [  64]  a21b02e8fca62643f45dcb5e1b9c4e99edd9
	│   ├── [4.0K]  ce
	│   │   └── [4.0K]  b1
	│   ├── [4.0K]  d5
	│   │   └── [4.0K]  8b
	│   │       └── [  34]  a9d1941bf359d86359ef27eccc9e21c4c178
	│   └── [4.0K]  f7
	│       └── [4.0K]  c5
	│           └── [  64]  9ab6f25820e9eb0cb72fa2219f9c643d44b5
	└── [4.0K]  series
		└── [4.0K]  0b
			└── [4.0K]  b6
				└── [4.0K]  f0edadfbce09627ee4d8d9e48c153e70236b
					└── [4.0K]  changelog
						└── [ 159]  0000000000000000000000000000000000000000.17449da9cde-9afa8506207c6a97f83441949f020abf89505fb7

	18 directories, 8 files
	bch@wsl:/tmp/data-dir$
	bch@wsl:/tmp/data-dir$ jensen squash
	bch@wsl:/tmp/data-dir$ jensen gc
	1 frames deleted
	bch@wsl:/tmp/data-dir$ tree -h
	.
	├── [  96]  input-corrected.csv
	├── [  84]  input.csv
	├── [4.0K]  registry
	│   └── [4.0K]  changelog
	│       └── [ 149]  0000000000000000000000000000000000000000.17449db845a-912e36458aa91d47958aa3e7db5768a53edd529c
	├── [4.0K]  segment
	│   ├── [4.0K]  2c
	│   │   └── [4.0K]  ff
	│   ├── [4.0K]  5e
	│   │   └── [4.0K]  66
	│   │       └── [  64]  a21b02e8fca62643f45dcb5e1b9c4e99edd9
	│   ├── [4.0K]  ce
	│   │   └── [4.0K]  b1
	│   ├── [4.0K]  d5
	│   │   └── [4.0K]  8b
	│   │       └── [  34]  a9d1941bf359d86359ef27eccc9e21c4c178
	│   ├── [4.0K]  f0
	│   │   └── [4.0K]  c8
	│   │       └── [  97]  641e2db634661a29359aa925787d9bae2ebb
	│   └── [4.0K]  f7
	│       └── [4.0K]  c5
	│           └── [  64]  9ab6f25820e9eb0cb72fa2219f9c643d44b5
	└── [4.0K]  series
		└── [4.0K]  0b
			└── [4.0K]  b6
				└── [4.0K]  f0edadfbce09627ee4d8d9e48c153e70236b
					└── [4.0K]  changelog
						└── [ 159]  0000000000000000000000000000000000000000.17449da9cde-9afa8506207c6a97f83441949f020abf89505fb7

	20 directories, 8 files

