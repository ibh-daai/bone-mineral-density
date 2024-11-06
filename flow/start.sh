#!/bin/bash

# Allow prefect server to start
sleep 5

# deploy prefect-flows
prefect deployment build main.py:extract_studies \
                      -n bmd \
                      -q bmd-pool \
                      -o prefect.yaml \
                      --skip-upload \
                      --apply

# start the agent
prefect agent start -q 'bmd-pool'