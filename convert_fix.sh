#!/bin/bash

for((i=2021;i>=2009;i--));
do
python convert_fix.py -y $i
done