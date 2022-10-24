# renterd-benchmark

## Overview

Simple tool to benchmark `renterd`'s performance. By default it creates 20 contracts and uploads 50 64kib files, uploaded to random hosts. It will then try and download the dataset using 1, 4, 16 and 64 threads and print percentile stats.

## Usage

The contracts are created using a renter key that needs to be located in the data directory, which defaults to `data/renter.key`.  
If no key file is found, a random one is generated.

To communicate with renterd you need to specify the address at which the API is listening as well as a password.  
These default to `http://127.0.0.1:9980/api` and `test` respectively.

### Parameters

- `addr`: renterd API url (default `http://127.0.0.1:9980/api`)
- `contracts`: number of contracts (default 20)
- `data-dir`: data directory (defaults to `data` in the current working directory)
- `files`: number of files (default 50)
- `min-shards`: number of min shards (default 2)
- `num-shards`: number of shards (default 5)
- `password`: renderd API password (default `test`)

## Example

```sh
renterd-benchmark --contracts 10 --files 10 --min-shards 2 --num-shards 5
```