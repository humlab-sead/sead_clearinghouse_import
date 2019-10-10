#!/bin/bash

set -e  # Exit script on any error

# if [ ! -h ./input ]; then
#     ln -s /mnt/c/Users/roma0050/Google\ Drive\ \(roma0050\@gapps.umu.se\)/Project/Public/VISEAD\ \(Humlab\)/SEAD\ Ceramics\ \&\ Dendro/input input
# fi

# if [ ! -h ./output ]; then
#     ln -s /mnt/c/Users/roma0050/Google\ Drive\ \(roma0050\@gapps.umu.se\)/Project/Public/VISEAD\ \(Humlab\)/SEAD\ Ceramics\ \&\ Dendro/output output
# fi

pipenv run python src/process.py --host 130.239.1.181 --dbuser clearinghouse_worker --input-folder ./input --output-folder ./output $@
