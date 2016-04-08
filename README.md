# Driver performance tests for libbson and  libmongoc

This is a C language implementation of the MongoDB standard driver performance
benchmark suite.

## Dependencies

Install libbson and libmongoc according to their instructions:

* [Install libbson](https://api.mongodb.org/libbson/current/installing.html).
* [Install libmongoc](https://api.mongodb.org/c/current/installing.html).

## Test data

The test data for the MongoDB driver performance benchmarks will be uploaded to
a public location; for now, download it from the corporate Google Drive to a
local directory.

## Build

Build the `mongo-c-performance` executable with [CMake](https://cmake.org/).

## Run

Run `mongo-c-performance` and pass the test data path:

```
./mongo-c-performance test-data-dir
```

Or run specific benchmarks:

```
./mongo-c-performance test-data-dir TestFlatEncoding TestDeepEncoding
```

The output is comma-separated values. The first column is the test name, the
second is its median duration in seconds.
