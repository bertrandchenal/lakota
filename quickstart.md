

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

	bch@wsl:/tmp/data-dir$ jensen create temp_bxl "timestamp:M8[s]" "value:f8"
	bch@wsl:/tmp/data-dir$ tree -h .
	.
	├── [  96]  input-corrected.csv
	├── [  84]  input.csv
	├── [4.0K]  registry
	│   └── [4.0K]  changelog
	│       └── [ 197]  0000000000000000000000000000000000000000.336fbf9a5c24161f995fbed84cf2643280f2b6b1
	└── [4.0K]  segment
		├── [4.0K]  44
		│   └── [  87]  80f5cc9d41319a260446b83413d2e3ec317ea0
		└── [4.0K]  d5
			└── [  16]  8ba9d1941bf359d86359ef27eccc9e21c4c178

	5 directories, 5 files


Some content:

	bch@wsl:/tmp/data-dir$ cat input.csv
	2020-06-22,25
	2020-06-23,24
	2020-06-24,27
	2020-06-25,31
	2020-06-26,32
	2020-06-27,30


First write:

	bch@wsl:/tmp/data-dir$ tree -h .
	.
	├── [  96]  input-corrected.csv
	├── [  84]  input.csv
	├── [4.0K]  registry
	│   └── [4.0K]  changelog
	│       └── [ 197]  0000000000000000000000000000000000000000.336fbf9a5c24161f995fbed84cf2643280f2b6b1
	├── [4.0K]  segment
	│   ├── [4.0K]  44
	│   │   └── [  87]  80f5cc9d41319a260446b83413d2e3ec317ea0
	│   ├── [4.0K]  5e
	│   │   └── [  64]  66a21b02e8fca62643f45dcb5e1b9c4e99edd9
	│   ├── [4.0K]  ce
	│   │   └── [  64]  b1c30243befa63f147509f83e0458cc906e69e
	│   └── [4.0K]  d5
	│       └── [  16]  8ba9d1941bf359d86359ef27eccc9e21c4c178
	└── [4.0K]  series
		└── [4.0K]  0b
			└── [4.0K]  b6f0edadfbce09627ee4d8d9e48c153e70236b
				└── [4.0K]  changelog
					└── [ 220]  0000000000000000000000000000000000000000.44a7b9a6d34e04e29ba9664d1f9c76b40541e918

	11 directories, 8 files


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
	bch@wsl:/tmp/data-dir$ tree -h .
	.
	├── [  96]  input-corrected.csv
	├── [  84]  input.csv
	├── [4.0K]  registry
	│   └── [4.0K]  changelog
	│       └── [ 197]  0000000000000000000000000000000000000000.336fbf9a5c24161f995fbed84cf2643280f2b6b1
	├── [4.0K]  segment
	│   ├── [4.0K]  44
	│   │   └── [  87]  80f5cc9d41319a260446b83413d2e3ec317ea0
	│   ├── [4.0K]  5e
	│   │   └── [  64]  66a21b02e8fca62643f45dcb5e1b9c4e99edd9
	│   ├── [4.0K]  ce
	│   │   └── [  64]  b1c30243befa63f147509f83e0458cc906e69e
	│   ├── [4.0K]  d5
	│   │   └── [  16]  8ba9d1941bf359d86359ef27eccc9e21c4c178
	│   └── [4.0K]  f7
	│       └── [  64]  c59ab6f25820e9eb0cb72fa2219f9c643d44b5
	└── [4.0K]  series
		└── [4.0K]  0b
			└── [4.0K]  b6f0edadfbce09627ee4d8d9e48c153e70236b
				└── [4.0K]  changelog
					├── [ 220]  0000000000000000000000000000000000000000.44a7b9a6d34e04e29ba9664d1f9c76b40541e918
					└── [ 220]  44a7b9a6d34e04e29ba9664d1f9c76b40541e918.e55e6a4023c0353c215f3548af986bd69b3388f3

	12 directories, 10 files


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
	bch@wsl:/tmp/data-dir$ tree -h .
	.
	├── [  96]  input-corrected.csv
	├── [  84]  input.csv
	├── [4.0K]  registry
	│   └── [4.0K]  changelog
	│       └── [ 197]  0000000000000000000000000000000000000000.336fbf9a5c24161f995fbed84cf2643280f2b6b1
	├── [4.0K]  segment
	│   ├── [4.0K]  44
	│   │   └── [  87]  80f5cc9d41319a260446b83413d2e3ec317ea0
	│   ├── [4.0K]  5e
	│   │   └── [  64]  66a21b02e8fca62643f45dcb5e1b9c4e99edd9
	│   ├── [4.0K]  65
	│   │   └── [  72]  309109f16c1d02f51e1a96ba65b2664d8dedfc
	│   ├── [4.0K]  ce
	│   │   └── [  64]  b1c30243befa63f147509f83e0458cc906e69e
	│   ├── [4.0K]  cf
	│   │   └── [  72]  9e30d81a187f3a3981a08f569ba9f966410c7c
	│   ├── [4.0K]  d5
	│   │   └── [  16]  8ba9d1941bf359d86359ef27eccc9e21c4c178
	│   └── [4.0K]  f7
	│       └── [  64]  c59ab6f25820e9eb0cb72fa2219f9c643d44b5
	└── [4.0K]  series
		└── [4.0K]  0b
			└── [4.0K]  b6f0edadfbce09627ee4d8d9e48c153e70236b
				└── [4.0K]  changelog
					└── [ 220]  0000000000000000000000000000000000000000.05bdcd7b0c4e83243908d45e6d4f405a0ffd4290
    14 directories, 11 files


Garbage collection

	bch@wsl:/tmp/data-dir$ jensen gc
	3 segments deleted
	bch@wsl:/tmp/data-dir$ tree -h .
	.
	├── [  96]  input-corrected.csv
	├── [  84]  input.csv
	├── [4.0K]  registry
	│   └── [4.0K]  changelog
	│       └── [ 197]  0000000000000000000000000000000000000000.336fbf9a5c24161f995fbed84cf2643280f2b6b1
	├── [4.0K]  segment
	│   ├── [4.0K]  44
	│   │   └── [  87]  80f5cc9d41319a260446b83413d2e3ec317ea0
	│   ├── [4.0K]  5e
	│   ├── [4.0K]  65
	│   │   └── [  72]  309109f16c1d02f51e1a96ba65b2664d8dedfc
	│   ├── [4.0K]  ce
	│   ├── [4.0K]  cf
	│   │   └── [  72]  9e30d81a187f3a3981a08f569ba9f966410c7c
	│   ├── [4.0K]  d5
	│   │   └── [  16]  8ba9d1941bf359d86359ef27eccc9e21c4c178
	│   └── [4.0K]  f7
	└── [4.0K]  series
		└── [4.0K]  0b
			└── [4.0K]  b6f0edadfbce09627ee4d8d9e48c153e70236b
				└── [4.0K]  changelog
					└── [ 220]  0000000000000000000000000000000000000000.05bdcd7b0c4e83243908d45e6d4f405a0ffd4290

	14 directories, 8 files
