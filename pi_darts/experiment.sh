#!/usr/bin/env bash

threads=256000
wgSize=2
repeat=100000

if [ ! -z "$1" ]
then
    threads=$1
fi

if [ ! -z "$2" ]
then
    wgSize=$2
fi

run(){
    echo ---- threads=${threads} wgSize=${wgSize} repeat=${repeat} ---->> result.txt
    for ((i=0;i<5;i++)); do
        echo `java -jar ./bin/artifacts/pi_darts_jar/pi_darts.jar ${threads} ${wgSize} ${repeat}` | sed 's/.*Done in \([0-9]\{3,\}\).*/\1/g' >> result.txt
    done
    echo threads=${threads} wgSize=${wgSize} repeat=${repeat} finished
}

question1(){
    while ((wgSize <= 1024)); do
        run
        ((wgSize=wgSize*2))
    done
}

question2(){
    wgSize=128
    threads=256

    while ((threads <= 32768)); do
        run
        ((threads=threads*2))
    done
}


question1
