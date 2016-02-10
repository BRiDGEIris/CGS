Creating docker with all CDH components & CGS
=============================================

Docker scripts in this directory spawns a CentOS 6 docker VM and installs all the CDH components (Hadoop, Spark, Hbase, Hive, Impala, Hue, Zookeeper, Oozie etc.).
Then, it installs the CGS system.

## How to build the cdh container?

```bash
docker build -t docker-cdh54 .
```

## How to run the cdh container?
```bash
docker run -v /root/docker:/root -p 8042:8042 -p 8088:8088 -p 8020:8020 -p 8888:8888 -p 11000:11000 -p 11443:11443 -p 9090:9090 -d -ti --privileged=true docker-cdh54
```
Wait a little as the build can take several minute to download each image and packages to rebuild CDH.

## How to open an ssh session inside the cdh?
```bash
docker ps
```
the command return the list of Containers. Select the Container ID matching the CDH container that was started.
=> 922ac2f47d93 ... or any similar Container Id

```bash
docker attach 922ac2f47d93
```

## How to start Hadoop once you go inside the container for the first time?
```bash
./usr/bin/cdh_centos_startup_script.sh
```

## How to install CGS ?
Go to 0.0.0.0:8888 and create an account with username "cloudera" and password "cloudera".
Then, install CGS (it will automatically add small samples from 1000G based on the database
structure of Highlander.
```bash
cd /home/cgs-apps/ && ./installCGSInDocker.sh
```

## How to use CGS ?
Modify the permissions to access your user directory by going to the Hue Interface and selecting the "File Browser". Then go /user, select your directory and set its permission at 666.

Put a vcf inside your user directory:
```bash
hdfs dfs -put /home/cgs-apps/apps/variants/tests/chr1_small.vcf /user/cloudera/
```

Inside the Hue interface, go to "Other apps" > "CGS". Then go to the section "Sample". There
you should see the file "chr1_small.vcf". Click on the button to import directly. It will take some time as for development we prefer to not do the import asynchronously (just 2 lines to uncomment if you want to make the import asynchronous). Note also that we do not have all MySQL databases needed in this Docker to be able to annotate the variants, so they will not be annotated. 

Once you have imported the data through the Hue interface, you have an example to access the CGS api at /home/cgs-apps/access.py.

## How to use Highlander?
Samples from 1000G were directly imported in Impala during the installation. The table with the variant is default.highlander_variant. The MySQL database for Highlander is not installed in this Docker and should be installed manually elsewhere.





