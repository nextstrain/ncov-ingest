#!/bin/bash

#SBATCH --output=cluster_logs/%j.out                 # where to store the output ( %j is the JOBID )
#SBATCH --error=cluster_logs/%j.err                  # where to store error messages

# activate conda environment
source $HOME/miniconda3/etc/profile.d/conda.sh
. $HOME/.nvm/nvm.sh

conda activate nextstrain
nvm use node

{exec_job}


