#!/bin/bash
#BSUB -J OCR_pipeline[1-16]                 # run 20 jobs in parallel
#BSUB -o logs/output_%I.out                 # stdout per task
#BSUB -e logs/error_%I.err                  # stderr per task
#BSUB -n 1
#BSUB -W 48:00
#BSUB -M 12000
#BSUB -R "rusage[mem=12000]"

# Load Python (Harvard FAS RC has modules)
module load python/3.10

# Activate venv if needed
# source ~/myvenv/bin/activate

echo "Running OCR job index $LSB_JOBINDEX"

YEARS_FILE="years.txt"

python ocr_pipeline_unzipped.py \
  --year-list-file "$YEARS_FILE" \
  --job-index "$LSB_JOBINDEX" \
  --ocr-concurrency 2
