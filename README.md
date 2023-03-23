# renterd-benchmark

This tool was used to benchmark `renterd`'s download perfomance. 

Its usage is quite straightforward, it will upload objects of a certain configurable file size to your node using a predictable filename format.

By default the files look like `/renterd-benchmark/file_40.0MiB_5`, changing the `id` allows you to benchmark using different datasets, avoiding having to reupload your dataset every time.

## Usage

```bash
Usage of renterd-benchmark:
  -background-ul
        upload small files in the background while performing download benchmarks
  -busAddr string
        bus address (default "http://127.0.0.1:9980/api/bus")
  -busPassw string
        bus password (default "test")
  -files int
        number of files in the dataset (default 100)
  -id string
        benchmark identifier (default "renterd-benchmark")
  -min-shards int
        number of min shards (default 10)
  -size int
        size of files to upload (default 41943040) // 40MiB
  -threads string
        comma separated list of thread counts (default "1,4,16")
  -total-shards int
        number of shards (default 30)
  -ul-threads int
        number of threads the uploader uses (default 1)
  -workerAddrs string
        comma separated list of worker addresses (default "http://127.0.0.1:9980/api/worker")
  -workerPassws string
        comma separated list of worker passwords (default "test")
```