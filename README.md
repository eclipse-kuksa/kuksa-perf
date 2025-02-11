# Eclipse Kuksa databroker-perf

Performance measurement app for KUKSA databroker. It measures end-to-end latencies between triggering ends (e.g. provided signals or actuation requests) and receiving ends (e.g. subscribing signals or picking up actuation requests).

```
[00:00:05] [================================================================================================================]       5/5       seconds

Global Summary:
  API: kuksa.val.v2
  Total elapsed seconds: 5
  Skipped test seconds: 2
  Total signals: 101 signals
  Sent: 412014 signal updates
  Skipped: 160207 signal updates
  Received: 251807 signal updates
  Throughput: 83935 signal/second
  95% in under: 0.294 ms
  Fastest:   0.056 ms
  Slowest:   4.187 ms
  Average:   0.225 ms

Latency histogram:
    0.188 ms [247553] |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
    0.563 ms [1475 ] |∎
    0.938 ms [143  ] |
    1.313 ms [1083 ] |
    1.688 ms [209  ] |
    2.063 ms [70   ] |
    2.438 ms [242  ] |
    2.813 ms [514  ] |
    3.188 ms [219  ] |
    3.563 ms [119  ] |
    3.938 ms [173  ] |
    4.313 ms [7    ] |

Latency distribution:
  10% in under 0.154 ms
  25% in under 0.180 ms
  50% in under 0.199 ms
  75% in under 0.224 ms
  90% in under 0.263 ms
  95% in under 0.294 ms
  99% in under 1.205 ms



```

# Local Setup

## Build databroker-perf binary

```
cargo build --release
```

## Start databroker (Docker)

```
docker run -it --rm -p 55555:55555 ghcr.io/eclipse-kuksa/kuksa-databroker:main --insecure --enable-databroker-v1
```

If running on MacOS:

```
docker run -it --rm -p 55556:55556 ghcr.io/eclipse-kuksa/kuksa-databroker:main --insecure --enable-databroker-v1 --port 55556
```

## Start databroker (Binary)

Use binary from [kuksa-databroker repository](https://github.com/eclipse-kuksa/kuksa-databroker)

```
cargo build --release
```

```
./target/release/databroker --vss data/vss-core/vss_release_4.0.json --enable-databroker-v1
```

If running on MacOS:

```
./target/release/databroker --vss data/vss-core/vss_release_4.0.json --enable-databroker-v1 --port 55556
```

## Usage databroker-perf

```
Usage: databroker-perf [OPTIONS]

Options:
  -d, --duration <SECONDS>              Number of seconds to run
      --api <API>                       Api of databroker [default: kuksa.val.v1] [possible values: kuksa.val.v1, kuksa.val.v2, sdv.databroker.v1]
      --direction <DIRECTION>           Api of databroker [default: read] [possible values: read, write]
      --host <HOST>                     Host address of databroker [default: http://127.0.0.1]
      --port <PORT>                     Port of databroker [default: 55555]
      --skip-seconds <SECONDS>          Seconds to run (skip) before measuring the latency
      --unix-socket <UNIX_SOCKET_PATH>  Unix socket path of databroker
      --detailed-output                 Print more details in the summary result
      --buffer-size <BUFFER_SIZE>       kuksa.val.v2 subscription buffer_size
      --test-data-file <FILE>           Path to test data file
  -v, --verbosity <LEVEL>               Verbosity level. Can be one of ERROR, WARN, INFO, DEBUG, TRACE [default: WARN]
  -h, --help                            Print help
  -V, --version                         Print version
```

Hint: `--direction write` is only supported for `--api kuksa.val.v2`

```
./target/release/databroker-perf [OPTIONS]
```

## Default test result output

By default, the group results output will be summarised and contracted as follows:
```
Global Summary:
  API: kuksa.val.v2
  Total elapsed seconds: 5
  Skipped test seconds: 2
  Total signals: 55 signals
  Sent: 5155 signal updates
  Skipped: 2069 signal updates
  Received: 3086 signal updates
  Signal/Second: 1028 signal/s
  95% in under: 0.215 ms
  Fastest:   0.067 ms
  Slowest:   0.295 ms
  Average:   0.140 ms

Latency histogram:
    0.070 ms [45   ] |∎∎
    0.090 ms [424  ] |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
    0.110 ms [537  ] |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
    0.130 ms [779  ] |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
    0.150 ms [462  ] |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
    0.170 ms [364  ] |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
    0.190 ms [180  ] |∎∎∎∎∎∎∎∎∎∎∎
    0.210 ms [164  ] |∎∎∎∎∎∎∎∎∎∎
    0.230 ms [85   ] |∎∎∎∎∎
    0.250 ms [33   ] |∎∎
    0.270 ms [5    ] |
    0.290 ms [8    ] |

Latency distribution:
  10% in under 0.092 ms
  25% in under 0.112 ms
  50% in under 0.132 ms
  75% in under 0.164 ms
  90% in under 0.199 ms
  95% in under 0.215 ms
  99% in under 0.253 ms

```

For a detailed output of the results, please enable the corresponding flag like:

```
./target/release/databroker-perf --detailed-output
```

## Group config file

Databroker-perf creates two new gRPC channels for each group: one for the triggering end and one for the receiving end.
Each triggering end will update its group signal values to the Databroker at the cycle time specified (in milliseconds) in the JSON configuration file provided.

i. e.
```
{
  "groups": [
    {
      "group_name": "Frame 1",
      "cycle_time_ms": 10,
      "signals": [
        {
          "path": "Vehicle.Speed"
        }
      ]
    },
    {
      "group_name": "Frame 2",
      "cycle_time_ms": 20,
      "signals": [
        {
          "path": "Vehicle.IsBrokenDown"
        },
        {
          "path": "Vehicle.IsMoving"
        },
        {
          "path": "Vehicle.AverageSpeed"
        }
      ]
    },
    ...
  ]
}
```

## Example with config file

```
./target/release/databroker-perf --test-data-file data/sensors/data_group_10.json
```

If running on MacOS:

```
./target/release/databroker-perf --test-data-file data/sensors/data_group_10.json --port 55556
```

## Example with API

```
./target/release/databroker-perf --api sdv.databroker.v1 --test-data-file data/sensors/data_group_10.json
```

If running on MacOS:

```
./target/release/databroker-perf --api sdv.databroker.v1 --test-data-file data/sensors/data_group_10.json --port 55556
```

## Contributing

Please refer to the [Kuksa Contributing Guide](CONTRIBUTING.md).

## License

Kuksa Databroker Perf tool is provided under the terms of the [Apache Software License 2.0](LICENSE).

## Contact

Please feel free to create [GitHub Issues](https://github.com/eclipse-kuksa/kuksa-perf/issues) for reporting bugs and/or ask questions in our [Gitter chat room](https://matrix.to/#/#kuksa-val_community:gitter.im).
