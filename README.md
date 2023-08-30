## Emulated Distributed File Storage for World Data Analysis
This project implements an Emulated Distributed File Storage (EDFS) system using Firebase for storage and SQL for metadata management. The system aims to facilitate the analysis of global happiness, unemployment, and GDP data. It provides a set of EDFS commands similar to common file system operations, enabling users to interact with the stored data efficiently. Additionally, a web-based interface is created using Flask, allowing users to search, analyze, and visualize the data stored in the EDFS using partition-based map and reduce operations.

App Link - https://happinessq.onrender.com/

## Table of Contents
- [Introduction](#introduction)
- [Features](#features)


## Introduction

The Emulated File Distribution System is a project that demonstrates the storage, management, and analysis of file data and metadata, combined with real-world data related to world happiness, unemployment, and GDP. The system uses both SQL and Firebase as database backends to provide flexibility and showcase different database technologies.

## Features

- Store and manage file data and metadata in SQL and Firebase databases.
- Command-line tools (`put`, `rm`, `getPartitionLocation`, `readPartition`, etc.) implemented in Python and Javascript for interacting with the system.
- Partition-based MapReduce for searching and analyzing stored data efficiently.
- Flask-based web application for visualizing analysis results.
 
