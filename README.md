# Driver performance tests for libbson and libmongoc

This is a C language implementation of the MongoDB standard driver performance
benchmark suite.

## Dependencies

Install libbson and libmongoc according to their [instructions|http://mongoc.org/libmongoc/current/installing.html].

## Test data

Run the included `download-test-data.py` to download test data. See `--help`
for options.

## Build

Build the `mongo-c-performance` executable with [CMake](https://cmake.org/).

## Run

Run `mongo-c-performance` and pass the test data path:

```
./mongo-c-performance performance-testdata
```

Or run specific benchmarks:

```
./mongo-c-performance performance-testdata TestFlatEncoding TestDeepEncoding
```

The output is space-separated values:

```
name, bytes_per_second
```

The first column is the test name.

The second column is the data size of the micro-benchmark divided by the median
of iteration runtimes.

The program runs each test for at least a minute, and runs it 100 times or five
minutes, whichever comes first. The third and fourth columns are informational:
how many iterations the test ran and the time spent running all iterations.
