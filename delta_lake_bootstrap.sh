#!/bin/bash 
sudo yum update -y 


if [ `sudo grep 'isMaster' /mnt/var/lib/info/instance.json | awk -F ':' '{print $2}' | awk -F ',' '{print $1}'` = 'false' ]; then
	echo "This is not the master node, exiting."
	exit 0
fi


sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh
##sudo pip-3.6 install boto3
sudo python3 -m pip install boto3
sudo chmod -R 757 /mnt/var/lib/hadoop/tmp
sudo aws s3 cp s3://vf-bdc-vb-euce1-dev-submission/delta/delta-core_2.11-0.6.1.jar /usr/lib/spark/jars/