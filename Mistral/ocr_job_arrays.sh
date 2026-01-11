#!/bin/bash
#BSUB -J OCR_pipeline_2010[1-1]            # run these years
#BSUB -o logs/output_2010.out                 # stdout per task
#BSUB -e logs/error_2010.err                  # stderr per task
#BSUB -n 1
#BSUB -W 48:00
#BSUB -M 6000
#BSUB -R "rusage[mem=6000]"
#BSUB -u jwalsh@hbs.edu
#BSUB -N
#BSUB -B

# Activate venv if needed
# source ~/myvenv/bin/activate

echo "Running OCR job index $LSB_JOBINDEX"

YEARS_FILE="years.txt"

python ocr_pipeline_unzipped.py \
  --year-list-file "$YEARS_FILE" \
  --job-index "$LSB_JOBINDEX" \
  --ocr-concurrency 1
