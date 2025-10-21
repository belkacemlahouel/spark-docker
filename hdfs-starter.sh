#!/bin/bash
set -e

# Start SSH daemon - Signed in as root
mkdir -p /run/sshd
/usr/sbin/sshd

su - hadoop

source ~/.bashrc

echo "export JAVA_HOME=$JAVA_HOME" >> /usr/local/hadoop/etc/hadoop/hadoop-env.sh
# RUN echo "export PDSH_RCMD_TYPE=ssh" >> /usr/local/hadoop/etc/hadoop/hadoop-env.sh

# Format namenode if not formatted yet
# Directories are already created via Dockerfile
if [ ! -d /home/hadoop/hdfs/namenode/current ]; then
    echo "HDFS directory not found - Formatting HDFS namenode..."
    hdfs namenode -format
fi

# Start HDFS daemons
start-dfs.sh

# Keep container running
tail -f /dev/null
