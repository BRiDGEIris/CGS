*IMPORTANT*: current code is for test purpose and it is far from perfect. If an error occurred, see the logs files AND debug.txt file (debug.txt is a personal file to manage exceptions)

##**Requirements**

Library to install for python: pycurl 
Data uploaded should be added to user/hdfs/data/. Subfolders are not taken into account yet.
Data compressed will be uploaded to user/hdfs/compressed_data/. 
Those folders should be manually created by the developer if they do not exist yet.

Change the privileges for the application does not work, do it for hue in general (dumb way to do it, but it works):
```
cd /usr/lib/
sudo find hue -type d -exec chmod 0777 {} \;
sudo find hue -type f -exec chmod 0777 {} \;
```

Database installation:
Launch hue, then go to http://quickstart.cloudera:8888/variants/database/initialize/


##**API**

###*Authentification:*
```
curl --data "username=cloudera&password=cloudera" -c "cookies.txt" -b "cookies.txt"
-X POST http://quickstart.cloudera:8888/accounts/login/
```

###*Request:*
 * Search for files containing the CUSTOMER_SAMPLE_ID and return {[customer_file_id, hdfs_file_path], ...}
    (note for the developers: the customer_sample_id in db is a little bit different than this one)
    ```    
    curl --data "sample_id=<CUSTOMER_SAMPLE_ID>" -c "cookies.txt" -b "cookies.txt" -X POST 
    http://quickstart.cloudera:8888/genomicAPI/api/files/search
	```

###*Resources*
Definition of the resources is largely inspired from Google Genomics (https://cloud.google.com/genomics/v1beta2/reference) but may slightly differ.
The resource type has one or more data representations and one or more methods.
The resource representations are detailed in the subsection [Resource representation](#readgroupsets) and the methods are described in the subsection [Methods](#methods).

#### <a name="resourceRepresentations">Resource representations</a> 

Resource types are:

- Samples
- Datasets
- Readgroupsets
- Variantsets
- Variants
- Callsets

**Samples**
Multiple samples can come from the same patient. The sample ID is also given in the read group set.  

| Property name | Value         | Description                              |
| -------       | ------------- | -----------                              |
| id            | string        | sample id                                |
| files         | list          | file list corresponding to this sample   |
| pathology     | string        | pathology of the patient                 |
| type          | string        | sample type (ex: Blood, Tissue or Cells) |
| datasetId     | string        | dataset id                               |


**Datasets**

A dataset is a collection of genomic data. It can contain many read group sets, variant sets, and call sets.

| Property name | Value         | Description |
| -------       | ------------- | ----------- |
| id            | string        | CGS id      |


**Readgroupsets**

A read group set is a logical collection of read groups, which are collections of reads produced by a sequencer. A read group set typically models reads corresponding to one sample, sequenced one way, and aligned one way. A read group set belongs to a dataset. A read group belongs to one read group set.

| Property name                     | Value         | Description                                                                                                                  |
| -------                           | ------------- | -----------                                                                                                                  |
| datasetId                         | string        | The dataset id                                                                                                               |
| filename                          | string        | The filename of the original source file for this read group set, if any.                                                    |
| id                                | string        | The read group set ID                                                                                                        |
| info                              | object        | A map of additional read group set information                                                                               |
| info.(key)[]                      | list          | A string which maps an array of values                                                                                       |
| name                              | string        | The read group set name. By default this will be initialized to the sample name of the sequenced data contained in this set. |
| readGroups[]                      | list          | The read group in this set.                                                                                                  |
| readGroups[].datasetId            | string        | The ID of the dataset this read group belong to.                                                                             |
| readGroups[].experiment           | nested object | The experiment used to generate this read group                                                                              |
| readGroups[].experiment.libraryId | string        | The library used as part of the experiment.                                                                                  |
| readGroups[].sampleId             | string        | The sample this read group's data was generated from.                                                                        |

**Variantsets**
A variant set is a collection of call sets and variants. It contains summary statistics of those contents. A variant set belongs to a dataset.

| Property name           | Value         | Description                                                |
| -------                 | ------------- | -----------                                                |
| id                      | string        | sample id                                                  |
| datasetId               | string        | The dataset to which this variant set belongs.             |
| metadata[]              | list          | The metadata associated with this variant set.             |
| metadata[].description  | string        | A description of this metadata                             |
| metadata[].info         | object        | Remaining structured key-value pairs                       |
| metadata[].info.(key)[] | list          | A string which maps to an array of values                  |
| metadata[].key          | string        | The top-level key                                          |
| metadata[].type         | string        | The type of data (INTEGER, FLOAT, FLAG, CHARACTER, STRING) |
| metadata[].value        | string        | The value field for simple metadata                        |


**Variants**
A variant represents a change in DNA sequence relative to a reference sequence. For example, a variant could represent a SNP or an insertion. Variants belong to a variant set. Each of the calls on a variant represent a determination of genotype with respect to that variant. For example, a call might assign probability of 0.32 to the occurrence of a SNP named rs1234 in a sample named NA12345. A call belongs to a call set, which contains related calls typically from one sample.

| Property name   | Value         | Description                                                                                                                                                                                                                                                                               |
| -------         | ------------- | -----------                                                                                                                                                                                                                                                                               |
| variantSetIds[] | list          | Exactly one variant set ID must be provided. Only variants from this variant set will be returned.                                                                                                                                                                                        |
| variantName     | string        | Only return variants which have exactly this name.                                                                                                                                                                                                                                        |
| callSetIds[]    | list          | Only return variant calls which belong to call sets with these ids. Leaving this blank returns all variant calls. If a variant has no calls belonging to any of these call sets, it won't be returned at all. Currently, variants with no calls from any call set will never be returned. |
| referenceName   | string        | Required. Only return variants in this reference sequence (ex: chr9, X).                                                                                                                                                                                                                  |
| start           | long          | Required. The beginning of the window (0-based, inclusive) for which overlapping variants should be returned.                                                                                                                                                                             |
| end             | long          | Required. The end of the window (0-based, exclusive) for which overlapping variants should be returned.                                                                                                                                                                                   |
| pageToken       | string        | The continuation token, which is used to page through large result sets. To get the next page of results, set this parameter to the value of nextPageToken from the previous response.                                                                                                    |
| pageSize        | integer       | The maximum number of variants to return.                                                                                                                                                                                                                                                 |
| maxCalls        | integer       | The maximum number of calls to return. However, at least one variant will always be returned, even if it has more calls than this limit.                                                                                                                                                  |


**Callsets**
A call set is a collection of variant calls, typically for one sample. It belongs to a variant set. 

| Property name   | Value         | Description                                                                                        |
| -------         | ------------- | -----------                                                                                        |
| id              | string        | The call set id                                                                                    |
| info            | object        | A map of additional call set information.                                                          |
| info.(key)[]    | list          | A string which maps to an array of values                                                          |
| name            | string        | The call set name                                                                                  |
| sampleId        | string        | The sample ID this call set corresponds to.                                                        |
| variantSetIds[] | list          | Exactly one variant set ID must be provided. Only variants from this variant set will be returned. |


#### <a name="methods">Methods</a> 
The methods for each resource type are listed below. 

**Samples**
For Samples Resource details, see the [resource representation page](#resourceRepresentations).

| Method  | HTTP request     | Description                    |
| ------- | -------------    | -----------                    |
| create  | POST /samples/  | Creates a new sample          |
| delete  | DELETE /samples/sampleId | Deletes a sample              |
| search  | POST /samples/search sampleId    | Search for samples matching criteria |


**Datasets**
For Datasets Resource details, see the [resource representation page](#resourceRepresentations).

| Method  | HTTP request     | Description                    |
| ------- | -------------    | -----------                    |
| create  | POST /datasets   | Creates a new dataset          |
| delete  | DELETE /datasets | Deletes a dataset              |
| list    | GET /datasets    | List datasets within a project |


**Readgroupsets**
For Readgroupsets Resource details, see the [resource representation page](#resourceRepresentations).

| Method  | HTTP request       | Description                 |
| ------- | -------------      | -----------                 |
| get     | GET /readgroupsets/*readGroupSetId* | Gets a read group set by ID |


**Variantsets**
For Variantsets Resource details, see the [resource representation page](#resourceRepresentations).

| Method  | HTTP request                     | Description              |
| ------- | -------------                    | -----------              |
| get     | GET /variantsets/*variantSetId*  | Gets a variant set by ID |
| import  | POST /variantsets/*variantSetId* | Creates variant data by asynchronously importing the provided information into HBase. |  

**Variants**
For Variants Resource details, see the [resource representation page](#resourceRepresentations).

| Method  | HTTP request                    | Description              |
| ------- | -------------                   | -----------              |
| get     | GET /variants/*variantId*       | Gets a variant by ID     |

**Callsets**
For Callsets Resource details, see the [resource representation page](#resourceRepresentations).

| Method  | HTTP request              | Description           |
| ------- | -------------             | -----------           |
| get     | GET /callsets/*callSetId* | Gets a call set by ID |





