#!/bin/bash  
  
for((i=2021;i>=2009;i--));  
do
python main.py -a content -y $i -l 30000 -t 5
done 