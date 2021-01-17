#!/bin/sh

#SBATCH --output=cluster_logs/%j.out                 # where to store the output ( %j is the JOBID )
#SBATCH --error=cluster_logs/%j.err                  # where to store error messages

# activate conda environment
conda activate nextstrain

{exec_job}


