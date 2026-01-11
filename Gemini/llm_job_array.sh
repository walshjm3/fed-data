#!/bin/bash
#BSUB -J gemini_run[1-6]
#BSUB -W 48:00
#BSUB -M 12000
#BSUB -R "rusage[mem=12000]"
#BSUB -o logs/output_%I.out                 # stdout per task
#BSUB -e logs/error_%I.err                  # stderr per task
#BSUB -u jwalsh@hbs.edu
#BSUB -N
#BSUB -B

# Map array index to year (offset by 1994)
YEAR=$((1994 + $LSB_JOBINDEX))
echo "Running year $YEAR"

# Run your Python script with YEAR as argument
python read_json.py --year $YEAR
