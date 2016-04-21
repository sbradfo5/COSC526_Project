#! /usr/bin/env bash

count=$(ps aux | grep -v "grep" | grep "streaming.py" | wc -l)

if [ $count -eq 0 ]
then
  echo "working"
  python streaming.py
fi
