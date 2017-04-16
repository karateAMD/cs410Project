#!/bin/sh

INPUT=$1
OUTPUT=$2
HOME=/home/hadoop
echo "Syncing...s3://cs410-project/apps/ with "$HOME"/"
sudo aws s3 sync --delete s3://cs410-project/apps/ $HOME/
sudo chmod a+x $HOME/*.sh
echo "Removing output directory "$OUTPUT
sudo aws s3 rm --recursive $OUTPUT
#echo -e '\nexport HADOOP_OPTS=-Xmx10g' >> $HOME/.bashrc
#echo -e '\nexport LD_LIBRARY_PATH='$HOME'/libstdc++.so.6' >> $HOME/.bashrc
#source $HOME/.bashrc
#sudo ulimit -n 24000
hadoop jar $HOME/summarystats-0.0.1-SNAPSHOT-jar-with-dependencies.jar $INPUT $OUTPUT
