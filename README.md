# MRBox :gift:
A user-friendly application for the management, processing and analysis of Big Data across local and remote platforms.

## Table of Contents
1. [About the Project](https://github.com/AthinaKyriakou/mrbox#about-the-project)
    * [Built With](https://github.com/AthinaKyriakou/mrbox#built-with)
    * [Features](https://github.com/AthinaKyriakou/mrbox#features)
2. [Getting Started](https://github.com/AthinaKyriakou/mrbox#getting-started)
    * [Prerequisites](https://github.com/AthinaKyriakou/mrbox#prerequisites)
    * [Installation](https://github.com/AthinaKyriakou/mrbox#installation)
3. [Usage](https://github.com/AthinaKyriakou/mrbox#usage)
4. [Documentation](https://github.com/AthinaKyriakou/mrbox#documentation)
5. [Contributing](https://github.com/AthinaKyriakou/mrbox#contributing)
6. [License](https://github.com/AthinaKyriakou/mrbox#license)
7. [Acknowledgements](https://github.com/AthinaKyriakou/mrbox#acknowledgements)

## About the Project
The analysis and processing of big data often requires combining remote and local platforms, services and 
cloud infrastructures. Due to the heterogeneity of these resources and the cost of transferring large-scale data, 
performing data-intensive tasks across them is typically a non-automated and laborious undertaking or requires the 
knowledge of specialized workflow tools by non-experts. 

The purpose of this application is to provide a **high-level and general picture** of the local and remote 
infrastructures, as well as to enable the user to **schedule**, **check locally**, **execute** and 
**remotely track operations** on data residing at any integrated platform. 

The application is structured according to the **filesystem hierarchy of the operating system** so that the monitoring 
will take place through seemingly local files and folders. The initial goal is to integrate **Hadoop** and **B2DROP** 
services and to be able to schedule **MapReduce jobs** with possibilities to expand to other platforms and types of 
data operations in the future.

![High Level Design](/images/high_level_design.jpg)

### Features
The main features currently implemented are:

* Monitoring of the local /mrbox folder to track the creation, modification, deletion and relocation of its 
files and sub folders

* Integration of Hadoop and implementation of one way synchronization between the local /mrbox folder and the /mrbox 
folder of the remote HDFS

* Simple scheduling of Map Reduce jobs from the local file system to be executed in a remote HDFS 

* Automated fetching of Map Reduce jobs' outputs locally, after their completion remotely

* Implementation of the ```.link``` file type. When the output generated by a Map Reduce job is larger than the file 
size that can be stored locally, a ```.link``` file is created locally with a link to the remote HDFS. The link can be 
used in the browser to view the output in HDFS

* Integration of the B2DROP platform using the ownCloud Desktop Client.

### Built With
* Python 3
* [Watchdog](https://pypi.org/project/watchdog/) 
* [hdfs3](https://hdfs3.readthedocs.io/en/latest/index.html)
* [sqlite3](https://docs.python.org/3/library/sqlite3.html)


## Getting Started
### Prerequisites
##### To use the app with a remote HDFS you need:
1. A remote Hadoop cluster that you can connect
2. The same version of Hadoop installed in your local machine
3. To update the `src/core/mrbox.conf` file with:
    * the inet of the remote Hadoop cluster (hdfsHost)
    * the port of HDFS (hdfsPort) in the remote cluster
    * the path of the Hadoop installation locally (hadoopPath)
    * the local path to the /mrbox folder (localPath)

Additionally, in the `mrbox.conf` you can change the path where the /mrbox folder will be created on HDFS (hdfsPath) and
the size limit of the files that will be fetched locally from HDFS or other remote sources (localFileSizeMB).

##### To synchronize your local /mrbox folder with a folder in B2DROP: 
Follow the instructions [here](https://eudat.eu/services/userdoc/b2drop) to create a B2ACCESS account and mount B2DROP 
using the WebDAV client. Prior to run the MRBox application, start the ownCloud Desktop client on your local machine.
    
### Installation
###### To install Miniconda:
```bash
make setup 
```

###### To create a conda environment and install the project's requirements:
```bash
make build 
```
	
## Usage
###### To run the application:
```bash
make run 
```

###### To schedule a Map Reduce job to be executed in the remote Hadoop cluster:
1. Write a Mapper and a Reducer to your preferred programming language
2. Create a `.yaml` file outside the local /mrbox folder that contains:
    * mapper: absolute path to the Mapper's file in the local file system
    * reducer: absolute path to the Reducer's file in the local file system
    * input: path of the input data, relevant to the local /mrbox folder
    * output: path where the outputs will be created, relevant to the local /mrbox folder
    A template for the .yaml file can be found [here](/template.yaml).
3. Move the `.yaml` inside the local /mrbox folder. 

The Map Reduce job will be issued to the remote cluster. After its completion, the outputs will be automatically created
in the local output path specified. In case of large output files, a `.link` file containing their HDFS path will only 
be created locally. You can use this link in your browser to view these large output files on HDFS.

###### [_to add about file commands on files_]

###### For help:
```bash
make help 
```

## Documentation
Further documentation can be found [here](add_link).

## Contributing
_In open source, we feel strongly that to really do something well, you have to get a lot of people involved. 
~ Linus Torvalds_

Any contribution is highly valued and appreciated. To contribute:

1. Fork the project
2. Create your feature branch: ```git checkout -b feature/<newFeatureName>```
3. Commit your changes: ```git commit -m 'Add <newFeatureName>'```
4. Push to the branch: ```git push origin feature/<newFeatureName>```
5. Open a pull request

## License
Distributed under the MIT License. See `LICENSE` for more information.

## Acknowledgements
This project would not have been initiated and developed without the support of Mr. Iraklis-Angelos Klampanos and the 
Software & Knowledge Engineering Lab (SKEL) at NCSR “Demokritos”.







