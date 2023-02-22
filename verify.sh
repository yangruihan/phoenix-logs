#!/bin/bash

for((i=2021;i>=2009;i--));
do
python debug.py -y $i
done