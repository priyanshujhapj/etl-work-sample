# ETL

This repository contains demo ETL scripts which are tested, but are pushed into this directory with minor changes.

The script is my own work and is here for the very purpose to show the workings of ETL.

**All have the same Workflow**

* We make connection with `azure` blob clients
* We retrive the data from the datasource, either from some API or a Shared File System
* We process the data in the their respective ETL scripts and helper functions
* We upload the data into the blob
