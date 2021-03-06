#!/bin/bash
set -e

source ~/.bash_profile

# Number of processors to use for mpi
NPROC=16

cd /spark-dir/tars

# list the ztf_public folders
list_of_tar=`/usr/bin/ls -1 | grep .tar.gz`

for atar in ${list_of_tar[@]}; do
    # Name for the temporary folder to decompress data
    folder=$(echo $atar | cut -d'.' -f1)

    # decompress data -- skip if untar fails
    mkdir -p $folder
    if ! pigz -dc -p 4 $atar | tar xf - -C $folder; then
        echo "Failed $atar"
        rm -rf $folder
        mv $atar /spark-dir/bad/
        continue
    fi

    # Get YYYY/MM/DD
    NIGHT="$(echo $folder | cut -d'_' -f3)"
    YEAR=${NIGHT:0:4}
    MONTH=${NIGHT:4:2}
    DAY=${NIGHT:6:2}

    out=/spark-dir/ztf_alerts/raw/year=$YEAR/month=$MONTH/day=$DAY
    mkdir -p $out
    echo "data will be stored at $out"

    # merge files
    mpirun --allow-run-as-root -n $NPROC python /home/centos/fink_ztf_recovery/merge_avro_to_parquet_mpi.py -i $folder -o $out > /spark-dir/logs/$NIGHT

    # remove the initial tar.gz
    rm $atar

    # Remove the initial folder to avoid filling disks
    # ideally check the processing goes OK before
    rm -rf $folder
done
