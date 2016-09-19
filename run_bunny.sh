#!/bin/bash
python bunny-input.py &
python bunny-response.py &
while true; do sleep 10; done