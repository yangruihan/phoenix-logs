#!/bin/bash

for((i=2021;i>=2009;i--));
do
python main.py -a content -y $i -l 200000 -t 16
done