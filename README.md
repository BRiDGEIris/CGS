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

#### The *variants* app - Install

For now, you need to go through additional steps to run this app (not included in installCGSapps.py yet):

0. `sudo python installCGSapps.py variants`
1. `sudo easy_install pip`
2. `sudo pip install ordereddict` (if you have python < 2.7)
3. `sudo pip install counter` (if you have python < 2.7)
4. Download PyVCF on https://pypi.python.org/pypi/PyVCF, decompress it and go inside the folder
5. `sudo python setup.py install `
6. Allow the app to create temporary files (only for dev): `chmod -R 777 /usr/lib/bin/hue`
7. Initialize the database by going to: http://quickstart.cloudera:8888/variants/database/initialize/

If you have problems with hue permissions, or that installCGSapps.py does not seem to restart the views.py after you modified it, you can try the following command in your virtual machine (not recommended in production, just for debug)
`find /usr/lib/hue -type d -exec chmod 777 {} \;`

HBase might not be available when you resume your VM, thus CGS will not work correctly. In that case:

- `sudo service hbase-master restart`
- `sudo service hbase-regionserver restart`

#### The *variants* app - Importing data
For now the import of vcf is using the local disk of the node where Hue is installed, because of that be careful to have enough free space on your hard drive for files larger than 10Go.

- Upload your vcf through the Hue interface inside your user directory
- Go to cgs/sample
- Select your vcf file then click on "Import directly"
- The import is in progress, and the data will be available soon through Impala/Hive and HBase

Note: For now, only one import of vcf file per user will work (do not launch simultaneous imports). It will be improved in later versions.

#### The *variants* app - Querying data

CGS implements almost the same interface to data as Google Genomics. Thanks to that, you can use their [**documentation**](https://cloud.google.com/genomics/v1beta2/reference/). Only the 'variants' section is supported yet.

- Accessing a single variant: http://quickstart.cloudera:8888/variants/api/variants/<pk> For example: http://quickstart.cloudera:8888/variants/api/variants/ulb|0|1|10177|A/
- Looking through variants like Google Genomics (see [**doc**](https://cloud.google.com/genomics/v1beta2/reference/variants/search) to structure your request correctly) is accessible through a POST query at http://quickstart.cloudera:8888/variants/api/variants/search/.
If you do not submit any field, you can modify directly the code in api.py at VariantDetail to be able to test easily through a GET query (for dev only).
- Highlander has a dedicated access to query data according to its table structure through http://quickstart.cloudera:8888/variants/api/variants/highlander_search/. It only supports a very limited range of SELECT queries and it will not always return the same data as it would do for the Highlander table. For example a `select count(*)` will count the number of variants in CGS, but in Highlander it would count the number of calls. To modify the behavior of the queries, contribution from Highlander developers is needed.
  - The data sent to CGS should be a POST with the following fields:
    - method: "SELECT"
    - fields: "field1, field2, ..." or "count(*)"
    - condition: "field1 = value1 AND field2 != value2 ..."
    - limit: integer
    - offset: integer
    - order-by: "field" (mandatory if an offset > 0 is given)

### App 2
*There is no other app yet ... please do not hesitate if you want to contribute. 
