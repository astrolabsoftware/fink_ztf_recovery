#!/bin/bash

list_of_files=`/usr/bin/ls -1 uris | grep .txt`

for file in ${list_of_files[@]}; do
    echo $file
    #aria2c -x16 -s10 -i $file
done

#aria2c -x16 -s10 -i uri-list.txt
