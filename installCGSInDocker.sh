#!/bin/bash

echo '> Install variants app from CGS'
sudo python installCGSapps.py variants

echo '> Install modules for the variants app'
sudo easy_install pip
sudo pip install ordereddict
sudo pip install counter
#sudo yum install -y git
# git clone https://github.com/jamescasbon/PyVCF
# sudo python PyVCF/setup.py install
sudo pip install pyvcf
sudo pip install djangorestframework==3.2.5
sudo pip install markdown
sudo pip install django-filter

echo 'Put Highlander data in hdfs'
# hdfs dfs -put highlander-data /user/cloudera/highlander-data

echo 'Import Highlander data in MySQL database'
# TODO

echo 'Import Highlander data in Impala database'
# TODO


echo '> Install database by going on: http://quickstart.cloudera:8888/variants/database/initialize'

