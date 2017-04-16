#!/bin/sh

INPUT=$1
OUTPUT=$2
HOME=/home/hadoop
sudo aws s3 sync --delete s3://us-west-1-test-bucket/apps/ $HOME/
sudo chmod a+x $HOME/*.sh
#echo -e '\nexport HADOOP_OPTS=-Xmx10g' >> $HOME/.bashrc
#echo -e '\nexport LD_LIBRARY_PATH='$HOME'/libstdc++.so.6' >> $HOME/.bashrc
hadoop jar $HOME/amazon-review-summarizer-0.0.1-SNAPSHOT-jar-with-dependencies.jar $INPUT $OUTPUT


# hadoop jar amazon-review-summarizer-0.0.1-SNAPSHOT-jar-with-dependencies.jar s3://us-west-1-test-bucket/wordcount_input/ s3://us-west-1-test-bucket/wordcount_output/