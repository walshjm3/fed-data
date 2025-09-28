#!/bin/bash
#BSUB -J gemini_run_district[1-5]
#BSUB -W 48:00
#BSUB -M 12000
#BSUB -R "rusage[mem=12000]"
#BSUB -o logs/district_output_%I.out                 # stdout per task
#BSUB -e logs/district_error_%I.err                  # stderr per task
#BSUB -u jwalsh@hbs.edu
#BSUB -N
#BSUB -B

# This will not work for Boston because the json's are sorted within year subfolders.

DISTRICTS=("Atlanta" "Cleveland" "Dallas" "Minneapolis" "Richmond")

DISTRICT="${DISTRICTS[$LSB_JOBINDEX-1]}"
echo "Running for district: $DISTRICT"

# Run your Python script with YEAR as argument
python read_json.py --district $DISTRICT