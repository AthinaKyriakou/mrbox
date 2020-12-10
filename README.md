#MRBox
App for the management, processing and analysis of Big Data across local and remote platforms.

## Table of Contents
1. [About the Project](https://github.com/AthinaKyriakou/mrbox#about-the-project)
    * [Built With](https://github.com/AthinaKyriakou/mrbox#built-with)
    * [Features](https://github.com/AthinaKyriakou/mrbox#features)
2. [Getting Started](https://github.com/AthinaKyriakou/mrbox#getting-started)
    * [Prerequisites](https://github.com/AthinaKyriakou/mrbox#prerequisites)
    * [Installation](https://github.com/AthinaKyriakou/mrbox#installation)
3. [Usage](https://github.com/AthinaKyriakou/mrbox#usage)
4. [Contributing](https://github.com/AthinaKyriakou/mrbox#contributing)
5. [License](https://github.com/AthinaKyriakou/mrbox#license)
6. [Acknowledgements](https://github.com/AthinaKyriakou/mrbox#acknowledgements)

## About the Project
The analysis and processing of Big Data often requires combining remote and local platforms, services and 
cloud infrastructures. Due to the heterogeneity of these resources and the cost of transferring large-scale data, 
performing data-intensive tasks across them is typically a non-automated and laborious undertaking or requires the 
knowledge of specialized workflow tools by non-experts. 

The purpose of this application is to provide a high-level and general picture of the local and remote infrastructures,
as well as to enable the user to schedule, check locally, execute and remotely track operations on data residing at any 
integrated platform. 

The application is structured according to the **filesystem hierarchy of the operating system** so that the monitoring 
will take place through seemingly local files and folders. The initial goal is to integrate **Hadoop** and **B2DROP** 
services and to be able to schedule **MapReduce jobs** with possibilities to expand to other platforms and types of 
data operations in the future.

![High Level Design](/images/high_level_design.png)

### Features

### Built With
* Python 3
* Watchdog
* hdfs3
* sqlite3 db

## Getting Started
### Prerequisites
### Installation
	
## Usage
## Contributing
## License
## Acknowledgements








To install miniconda:
```bash
make setup
```
To create a conda environment and install the project's requirements:
```bash
make build 
```
To run the application:
```bash
make run 
```
For help:
```bash
make help 
```



