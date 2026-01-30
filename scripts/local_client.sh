#!/bin/bash

REQUEST_FILE='scripts/request'

input=$(tr -d '[:space:]' < "$REQUEST_FILE")

# Print hex dump
# echo -n "$input" | xxd -r -p | hexdump -C
echo -e "Running command: echo -n \"$input\" | xxd -r -p | nc localhost 9092 | hexdump -C\n"
echo -n "$input" | xxd -r -p | nc localhost 9092 | hexdump -C

