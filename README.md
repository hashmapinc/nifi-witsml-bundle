<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/Tempus_Logo_Black_with_TagLine.png" width="950" height="245" alt="Hashmap, Inc Tempus"/>

[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt) [![Build Status](https://travis-ci.org/hashmapinc/nifi-witsml-bundle.svg?branch=master)](https://travis-ci.org/hashmapinc/nifi-witsml-bundle)

# nifi-witsml-bundle

The Nifi-Witsml utilizes the WITSML Object Library SDK and the WITSML Client library to allow the Nifi Processor and Controller to connect and query WITSML server. It has support for WITSML 1.3.1.1 and 1.4.1.1 Servers. The implementation have an Controller service to control the connection to WITSML server and two processor, one to get meta-data of the channel and second to get data for particular Object.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Usage](#usage)

## Features

This library provide Nifi Controller and Processor :
* Controller connection to WITSML server and define the WITSML version
* GetObject Processor : to fetch the meta-data for the listed object from WITSML serverfor particular WellId and WellboreId.
* GetData Processor : to fetch the data for Log, MudLog and Trajectory Objects for a particular WellId and WellboreId

## Requirements

* JDK 1.8 at a minimum
* Maven 3.1 or newer
* Nifi 1.3.0
* WITSML ObjectLibrary
* WITSML ClientLibrary

## Getting Started
To build the library and get started first off clone the GitHub repository 

    git clone https://github.com/hashmapinc/nifi-witsml-bundle.git

Change directory into the nifi-witsml-bundle

    cd nifi-witsml-bundle
    
Execute a maven clean install

    mvn clean install
    
A Build success message should appear
      
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------
    [INFO] Total time: 8.329 s
    [INFO] Finished at: 2017-08-22T17:45:44+05:30
    [INFO] Final Memory: 30M/391M
    [INFO] ------------------------------------------------------------------------

A NAR file should be located in the following directory

    {repo_location}/nifi-witsml-bundle/nifi-nifiwitsml-nar/target
    
Copy this NAR file to the /lib directory and restart (or start) Nifi.

## Usage

#### Finding the WITSML Processor

Once NiFi is restarted the processors should be able to be added as normal, by dragging a processor onto the NiFi canvas. You can filter the long list of processors by typing WITSML, you will see two Processor with name GetObject and GetData.

Provide necessary property for respective processor and configure the controller service according to the WITSML server supported version.

