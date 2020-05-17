---
layout: home
title:  "Home"
section: "home"
technologies:
 - first: ["Scala", "sbt-microsites plugin is completely written in Scala"]
 - second: ["Spark", "sbt-microsites plugin uses SBT and other sbt plugins to generate microsites easily"]
---

[![Build Status](https://travis-ci.org/jpzk/mockedstreams.svg?branch=master)](https://travis-ci.org/jpzk/mockedstreams)   [![Codacy Badge](https://api.codacy.com/project/badge/Grade/8abac3d072e54fa3a13dc3da04754c7b)](https://www.codacy.com/app/jpzk/mockedstreams?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=jpzk/mockedstreams&amp;utm_campaign=Badge_Grade)

[EtlFlow] is a library that help with faster ETL development using Apache Spark. At a very high level, the library provides abstraction on top of Spark that makes it easier to develop ETL applications which can be easily Tested and Composed. This library has Google Bigquery support as both ETL source and destination.


## Requirements and Installation

This project is compiled with scala version 2.12.10 and works with Apache Spark versions 2.4.x. Available via [maven central](https://mvnrepository.com/artifact/com.github.tharwaninitin/etljobs). Add the latest release as a dependency to your project

**Maven**

    <dependency>
        <groupId>com.github.tharwaninitin</groupId>
        <artifactId>etlflow-core_2.12</artifactId>
        <version>0.7.7</version>
    </dependency>
    
**SBT**

    libraryDependencies += "com.github.tharwaninitin" %% "etlflow-core" % "0.7.7"

[Download Latest](https://github.com/tharwaninitin/etljobs/releases/tag/v0.7.7)
    

## Supported Libraries
* [Scala](https://www.scala-lang.org/)
* [Spark](https://spark.apache.org/docs/latest/)

## Version Compatibility

Please use the following dependant library versions.

| Technology           | Version     |
|----------------------|-------------|
| Scala                | 2.12.10     |
| Spark                | 2.4.4       |

