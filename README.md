# Eclipse Kuksa databroker-perf

Performance measurement app for KUKSA databroker.

```
[00:00:00]  [========================================================================================================]    1000/1000    iterations

Summary:
  API: SdvDatabrokerV1
  Elapsed time: 0.16 s
  Rate limit: None
  Sent: 1000 iterations * 1 signals = 1000 updates
  Skipped: 10 updates
  Received: 990 updates
  Fastest:   0.042 ms
  Slowest:   0.686 ms
  Average:   0.130 ms

Latency histogram:
    0.029 ms [689  ] |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
    0.087 ms [37   ] |∎∎∎∎∎∎∎
    0.145 ms [16   ] |∎∎∎
    0.203 ms [56   ] |∎∎∎∎∎∎∎∎∎∎
    0.261 ms [38   ] |∎∎∎∎∎∎∎
    0.319 ms [31   ] |∎∎∎∎∎
    0.377 ms [42   ] |∎∎∎∎∎∎∎∎
    0.435 ms [22   ] |∎∎∎∎
    0.493 ms [24   ] |∎∎∎∎
    0.551 ms [18   ] |∎∎∎
    0.609 ms [12   ] |∎∎
    0.667 ms [5    ] |

Latency distribution:
  10% in under 0.047 ms
  25% in under 0.049 ms
  50% in under 0.052 ms
  75% in under 0.177 ms
  90% in under 0.376 ms
  95% in under 0.477 ms
  99% in under 0.608 ms

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
  -i, --iterations <ITERATIONS>  Number of iterations to run [default: 1000]
      --api <API>                Api of databroker [default: kuksa.val.v1] [possible values: kuksa.val.v1, sdv.databroker.v1]
      --host <HOST>              Host address of databroker [default: http://127.0.0.1]
      --port <PORT>              Port of databroker [default: 55555]
      --skip <ITERATIONS>        Number of iterations to run (skip) before measuring the latency [default: 10]
      --interval <MILLISECONDS>  Minimum interval in milliseconds between iterations [default: 0]
      --config <FILE>            Path to configuration file
      --run-forever              Run the measurements forever (until receiving a shutdown signal)
  -v, --verbosity <LEVEL>        Verbosity level. Can be one of ERROR, WARN, INFO, DEBUG, TRACE [default: WARN]
  -h, --help                     Print help
  -V, --version                  Print version
```

```
./target/release/databroker-perf [OPTIONS]
```

## Example with config file

```
./target/release/databroker-perf --config configs/config.json
```

If running on MacOS:

```
./target/release/databroker-perf --config configs/config.json --port 55556
```

## Example with API

```
./target/release/databroker-perf --api sdv.databroker.v1 --config configs/config.json
```

If running on MacOS:

```
./target/release/databroker-perf --api sdv.databroker.v1 --config configs/config.json --port 55556
```

## Contributing

Please refer to the [Kuksa Contributing Guide](CONTRIBUTING.md).

## License

Kuksa Databroker Perf tool is provided under the terms of the [Apache Software License 2.0](LICENSE).

## Contact

Please feel free to create [GitHub Issues](https://github.com/eclipse-kuksa/kuksa-perf/issues) for reporting bugs and/or ask questions in our [Gitter chat room](https://matrix.to/#/#kuksa-val_community:gitter.im).
