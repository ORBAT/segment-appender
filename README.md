# segment-appender

Creates [druid](http://druid.io/) segment append tasks for a given data source and interval. Will create segments with
a configurable minimum and maximum size. Note that [sharded segments can't be appended](https://groups.google.com/forum/#!msg/druid-user/81XaOOnA6Ko/b5VbXV_98l8J),
at least in druid 0.8.3. Does **not** follow task success or failure, simply submits tasks.

segment-appender logs to stderr. A JSON array of overlord replies will be written to stdout.

## Installation

`npm install -g git://github.com/ORBAT/segment-appender.git#master`

## Usage

```
  Usage: segment-appender [options]

  Options:

    -h, --help                      output usage information
    -V, --version                   output the version number
    -b, --broker <host>             Druid broker address (required)
    -c, --coordinator <host>        Druid coordinator address (required)
    -o, --overlord <host>           Druid overlord address (required)
    -i, --interval <list>           ISO 8601 intervals to append. May be given multiple times (required)
    -d, --data-source <dataSource>  Druid data source to append (required)
    -r, --max-concurrent-reqs <n>   Maximum concurrent requests [10]
    -x, --max-segment-size <val>    Maximum segment size [900MB]
    -m, --min-segment-size <val>    Minimum segment size: smaller segments will not be created [500MB]
    -n, --dry-run                   Don't actually create the task
    -v, --verbose                   Be verbose
```

## Example usage

```
segment-appender --verbose --broker druid-broker.example.com:80 --coordinator druid-coordinator.example.com:80\
--overlord druid-overlord.example.com:80 --data-source some_source --interval 2016-03-01/2016-04-01
```

## Exit codes

* 1:   required command line parameter was missing
* 2:   uncaught error
* 3:   error making HTTP request to a druid server
* 4:   no segments found for given data source and interval