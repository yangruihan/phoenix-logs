#!/bin/bash  
  
for((i=2009;i<=2021;i++));  
do
python main.py -a id -y $i
done 