#!/bin/bash
wget https://github.com/philipperemy/FX-1-Minute-Data/raw/master/2000-Jun2019/bcousd/DAT_ASCII_BCOUSD_M1_2016.zip
unzip DAT_ASCII_BCOUSD_M1_2016.zip
head DAT_ASCII_BCOUSD_M1_2016.csv
head DAT_ASCII_BCOUSD_M1_2016.txt
wc -l DAT_ASCII_BCOUSD_M1_2016.*
gzip DAT_ASCII_BCOUSD_M1_2016.csv
ls -al *
zcat DAT_ASCII_BCOUSD_M1_2016.csv.gz | head
zcat DAT_ASCII_BCOUSD_M1_2016.csv.gz | grep 201608 > bcousd_201608.csv
gzip bcousd_201608.csv
zcat bcousd_201608.csv.gz | head
zcat bcousd_201608.csv.gz | tail
echo "done fetching brent oil data for local testing"

