# cgs-apps
Genomics apps for accessing data, importing data, visualizing data, etc.

This package is part of the [**CGS**](https://github.com/jpoullet2000/cgs) project. 

This package provides apps that are plugins for the UI [Hue](http://gethue.com/).  his module also contains user interfaces that are available through the web interface. 

A description of the apps available in Hue are described below.

## Installation
To install a new app, download the zip file, decompress it and run from the decompressed directory `sudo python installCGSapps.py <appname1> <appname2>`.

## Apps

### The *variants* app
The *variants* app aims to deal with variants.
Here is a list of functionalities of this app:

- *Importing VCF files into a NoSQL database*: the VCF file is converted to a JSON, which is then converted to an AVRO file that is stored, the AVRO file is then loaded into HBase.
- *Querying variants from Impala*: a Hive Metastore is built upon HBase that can be then requested through Impala.
- *Importing patient/sample information*: a web interface allows the user to register information about a sample.

Those functionalities are available through the Hue interface and through external client using API. For more details about the API provided in this app, see [*here*](https://github.com/jpoullet2000/cgs-apps/blob/master/apps/variants/src/variants/static/help/index.md).   

For now, you need to go through additional steps to run this app (not included in installCGSapps.py yet)
0. `sudo python installCGSapps.py variants`
1. `sudo easy_install pip`
2. `sudo pip install ordereddict` (if you have python < 2.7)
3. `sudo pip install counter` (if you have python < 2.7)
4. Download PyVCF on https://pypi.python.org/pypi/PyVCF, decompress it and go inside the folder
5. `sudo python setup.py install `

If you have problems with hue permissions, or that installCGSapps.py does not seem to restart the views.py after you modified it, you can try the following command in your virtual machine (not recommended in production, just for debug)
`find /usr/lib/hue -type d -exec chmod 777 {} \;`
 
### App 2
*There is no other app yet ... please do not hesitate if you want to contribute. 
