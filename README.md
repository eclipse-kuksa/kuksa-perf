# Eclipse Kuksa databroker-perf

Performance measurement app for KUKSA databroker.

```
[00:00:10] Group: Frame 1 | Cycle(ms): 10 | Current latency: 0.519 ms [==============================================================================================================]      10/10      seconds
[00:00:09] Group: Frame 2 | Cycle(ms): 20 | Current latency: 0.562 ms [==============================================================================================================]      10/10      seconds
[00:00:09] Group: Frame 3 | Cycle(ms): 30 | Current latency: 0.732 ms [==============================================================================================================]      10/10      seconds

Global Summary:
  API: KuksaValV2
  Run seconds: 6
  Skipped run seconds: 2
  Total signals: 8 signals
  Sent: 2275 signal updates
  Skipped: 768 signal updates
  Received: 1507 signal updates
  Signal/Second: 376 signal/s
  95% in under: 0.915 ms
  Fastest:   0.208 ms
  Slowest:   3.575 ms
  Average:   0.623 ms

Latency histogram:
    0.153 ms [38   ] |
    0.459 ms [759  ] |∎∎∎∎∎∎∎
    0.765 ms [637  ] |∎∎∎∎∎∎
    1.071 ms [51   ] |
    1.377 ms [10   ] |
    1.683 ms [7    ] |
    1.989 ms [3    ] |
    2.295 ms [1    ] |
    2.601 ms [0    ] |
    2.907 ms [0    ] |
    3.213 ms [0    ] |
    3.519 ms [1    ] |

Latency distribution:
  10% in under 0.477 ms
  25% in under 0.535 ms
  50% in under 0.596 ms
  75% in under 0.682 ms
  90% in under 0.769 ms
  95% in under 0.908 ms
  99% in under 1.332 ms
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
  -r, --run-seconds <RUN_SECONDS>   Number of seconds to run [default: 8]
      --api <API>                   Api of databroker [default: kuksa.val.v1] [possible values: kuksa.val.v1, kuksa.val.v2, sdv.databroker.v1]
      --host <HOST>                 Host address of databroker [default: http://127.0.0.1]
      --port <PORT>                 Port of databroker [default: 55555]
      --skip-seconds <RUN_SECONDS>  Seconds to run (skip) before measuring the latency [default: 4]
      --detailed-output             Print more details in the summary result
      --test-data-file <FILE>       Path to test data file
      --run-forever                 Run the measurements forever (until receiving a shutdown signal)
  -v, --verbosity <LEVEL>           Verbosity level. Can be one of ERROR, WARN, INFO, DEBUG, TRACE [default: WARN]
  -h, --help                        Print help
  -V, --version                     Print version
```

```
./target/release/databroker-perf [OPTIONS]
```

## Default test result output

By default, the group results output will be summarised and contracted as follows:
```
Global Summary:
  API: KuksaValV2
  Run seconds: 6
  Skipped run seconds: 2
  Total signals: 8 signals
  Sent: 2275 signal updates
  Skipped: 768 signal updates
  Received: 1507 signal updates
  Signal/Second: 376 signal/s
  95% in under: 0.915 ms
  Fastest:   0.208 ms
  Slowest:   3.575 ms
  Average:   0.623 ms

Latency histogram:
    0.153 ms [38   ] |
    0.459 ms [759  ] |∎∎∎∎∎∎∎
    0.765 ms [637  ] |∎∎∎∎∎∎
    1.071 ms [51   ] |
    1.377 ms [10   ] |
    1.683 ms [7    ] |
    1.989 ms [3    ] |
    2.295 ms [1    ] |
    2.601 ms [0    ] |
    2.907 ms [0    ] |
    3.213 ms [0    ] |
    3.519 ms [1    ] |

Latency distribution:
  10% in under 0.477 ms
  25% in under 0.535 ms
  50% in under 0.596 ms
  75% in under 0.682 ms
  90% in under 0.769 ms
  95% in under 0.908 ms
  99% in under 1.332 ms
```

For a detailed output of the results, please enable the corresponding flag like:

```
./target/release/databroker-perf --detailed-output
```

## Group config file

Databroker-perf creates two new gRPC channels for each group: one for the provider and one for the subscriber.
Each provider will update its group signal values to the Databroker at the cycle time specified (in milliseconds) in the JSON configuration file provided.

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
    }
  ]
}
```

## Example with config file

```
./target/release/databroker-perf --test-data-file data/data_group_10.json
```

If running on MacOS:

```
./target/release/databroker-perf --test-data-file data/data_group_10.json --port 55556
```

## Example with API

```
./target/release/databroker-perf --api sdv.databroker.v1 --test-data-file data/data_group_10.json
```

If running on MacOS:

```
./target/release/databroker-perf --api sdv.databroker.v1 --test-data-file data/data_group_10.json --port 55556
```

## Contributing

Please refer to the [Kuksa Contributing Guide](CONTRIBUTING.md).

## License

Kuksa Databroker Perf tool is provided under the terms of the [Apache Software License 2.0](LICENSE).

## Contact

Please feel free to create [GitHub Issues](https://github.com/eclipse-kuksa/kuksa-perf/issues) for reporting bugs and/or ask questions in our [Gitter chat room](https://matrix.to/#/#kuksa-val_community:gitter.im).
