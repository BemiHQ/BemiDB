#!/bin/bash

PID=$(ps | grep "/exe/BemiDB sync" | grep -v grep | awk '{print $1}')

if ! ps -p "$PID" > /dev/null 2>&1; then
  echo "Error: Process with PID $PID does not exist"
  exit 1
fi

echo "PID    Time                 Mem"

max_memory=0

while true; do
  current_time=$(date "+%Y-%m-%d %H:%M:%S")
  current_memory=$(top -pid $PID -stats mem -l 1 | tail -n 1 | sed 's/[^0-9]*//g')

  if [ "$current_memory" -gt "$max_memory" ]; then
    max_memory=$current_memory
    printf "%s  %s  %sMB (%sMB new max)\n" "$PID" "$current_time" "$current_memory" "$max_memory"
  else
    printf "%s  %s  %sMB\n" "$PID" "$current_time" "$current_memory"
  fi

  sleep 1
done
