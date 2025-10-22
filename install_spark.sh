#!/bin/bash

set -e

SPARK_VERSION=3.5.7
HADOOP_VERSION=3
SPARK_HOME=/opt/spark

sudo apt update && sudo apt upgrade -y
sudo apt install -y openjdk-11-jdk scala wget curl python3 python3-pip python3-setuptools vim unzip

wget https://downloads.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz
tar -xvzf spark-$SPARK_VERSION-bin-hadoop3.tgz
sudo rm -rf /opt/spark
sudo mv spark-$SPARK_VERSION-bin-hadoop3 $SPARK_HOME
rm spark-$SPARK_VERSION-bin-hadoop3.tgz

echo "export SPARK_HOME=$SPARK_HOME" >> ~/.bashrc
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
echo 'export HADOOP_USER_NAME=hadoop' >> ~/.bashrc
echo "export PATH=\$JAVA_HOME/bin:\$SPARK_HOME/bin:\$PATH" >> ~/.bashrc
source ~/.bashrc

sudo apt install python3.12-venv
python3 -m venv ~/spark-env
source ~/spark-env/bin/activate
pip install --upgrade pip setuptools
pip install pyspark==${SPARK_VERSION} pandas numpy

# add jars to connect to s3
mkdir -p $SPARK_HOME/jars
wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar
wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar
wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.6/hadoop-auth-3.3.6.jar
wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs/3.3.6/hadoop-hdfs-3.3.6.jar
wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-annotations/3.3.6/hadoop-annotations-3.3.6.jar
wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar

# opt: install aws-cli - not needed since we provide auth through AWS Console
# curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
# unzip awscliv2.zip
# sudo ./aws/install
# aws configure

echo "Completed"
