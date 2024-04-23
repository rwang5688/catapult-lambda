#!/bin/bash

echo "[CMD] python lambda_function.py"
cd lambda
python ./lambda_function.py --test-event ../test-event.json
cd ..
