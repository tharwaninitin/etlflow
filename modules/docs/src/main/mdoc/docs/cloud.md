---
layout: docs
title: Etlflow Cloud
---

## Etlflow Cloud

Etlflow Cloud is one of component of etlflow library which we can use to define the steps related to GCP dataproc or AWS cluster like s3 sensor step, gcp put step, gcp dataproc cluster creation/deletion step etc. 

### Etlflow cloud library contains sub-components like :
* AWS : This module can get used when we want to read/write data to and from aws buckets. 
* GCP : This module can get used when we want to read/write data to and from gcp buckets.

To use etlflow cloud library in project add below setting in build.sbt file : 

```

lazy val docs = (project in file("modules/examples"))
        .settings(
         scalaVersion := "2.12.13",
         libraryDependencies ++= List("com.github.tharwaninitin" %% "etlflow-core" % "0.10.0"))
         
```

### STEP 1) Building an assembly jar for etlflow cloud
To build an assembly jar for an etlflow cloud library we need to perform some steps. Clone this git repo and go inside repo root folder and perform below steps: 
       
         
      > sbt
      > project cloud
      > assembly
      
* Inside root folder run 'sbt' command
* Inside sbt, Run 'project cloud' to set the current project as etlflow-cloud
* Once project gets set then run 'assembly' to build an assembly jar.       


### STEP 2) Copy the etlflow cloud assembly jar to gcs bucket using below command
 
      gsutil cp modules/cloud/target/scala-2.12/etlflow-cloud-assembly-x.x.x.jar gs://<BUCKET-NAME>/jars/examples
      
Replace x.x.x with the latest version [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-cloud_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-cloud)
    
## Example : 

Below is the step of cloud library using which we can run the query on hive cluster : 

```scala mdoc

import etlflow.etlsteps.{DPHiveJobStep, DPSparkJobStep}
import etlflow.utils.Executor.DATAPROC
import etlflow.etljobs.GenericEtlJob
import etlflow.EtlJobProps

case class EtlJob1Props (
  inpuut_query: String = "SELECT 1 AS ONE"
      ) extends EtlJobProps

case class EtlJob1(job_properties: EtlJob1Props) extends GenericEtlJob[EtlJob1Props] {

     val dpConfig = DATAPROC(
          sys.env("DP_PROJECT_ID"),
          sys.env("DP_REGION"),
          sys.env("DP_ENDPOINT"),
          sys.env("DP_CLUSTER_NAME")
      )
  
      val step = DPHiveJobStep(
          name  = "DPHiveJobStepExample",
          query = "SELECT 1 AS ONE",
          config = dpConfig,
      )

      val job = for {
         _ <- step.execute()
      } yield ()
}

```   
 