# Building the raw alert data lake

In case things go wrong, keep breathing, and follow instructions. Requirements:

- openmpi
- aria2c
- pigz
- pandas, pyarrow, numpy, fink-client, fastavro, mpi4py...

and have a good internet connection!

## List of observation nights

Go to [https://ztf.uw.edu/alerts/public](https://ztf.uw.edu/alerts/public), and copy the table in your clipboard (select lines, and copy). Then, just execute the `generate_uri.py` script

```bash
python generate_uri.py
```

-------- make yearly uris?

This will generate a pandas DataFrame from the clipboard, estimate the volume of data to transfer, and save all needed URLs to download data (one per observation night). For convenience, nights will be split by groups of 5, stored in the. folder `uris`.

## Download observation nights

Once you have the list of nights to transfer, it is time to transfer! We have a wrapper that do it for you:

```bash
./download.sh
```

----------- check if file has been already downloaded?

Under the hood, this uses [aria2c](https://aria2.github.io/manual/en/html/index.html) to speed-up the download. I managed to get stable transfer at 100 MB/s per file (we process 5 files simultaneously).

## Format files for Apache Spark

The data transfered is a compressed folder (`.tar.gz`) containing Apache Avro files. While this is perfect for an archive, this is not suitable for our processing afterwards. You need then to decompress the files, concatenate the data from individual Avro files into larger chunks, and save into Parquet files. by concatenating 3000 Avro files into one single Parquet file, we currently achieve a compression factor of about 1.6. In addition, files are saved into a partitioned structure (`year/month/day`) to speed-up subsequent operations. We have a wrapper for doing this:

```bash
./concat.sh
```

The wrapper will look for all `ztf_public*.tar.gz` files, and do the job. To speed-up the computation, we use `mpi` under the hood. The script will use by default 16 cores 

## Performances

TBD

<!--(~100,000 alerts in 15 seconds).-->
