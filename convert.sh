#!/bin/bash

for((i=2021;i>=2009;i--));
do
python convert.py -y $i -c 300000
done