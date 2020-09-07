---
layout: docs
title: Parallel Step Execution
---

## Http Step

**This page shows how to execute more than one steps in parallel**

### Example 1
Below is the sample Http Step GET example. 

```scala mdoc
        
import etlflow.etlsteps._
import etlflow.etlsteps.ParallelETLStep
import etlflow.etljobs.SequentialEtlJob
import etlflow.EtlJobProps
import etlflow.utils.Config
import etlflow.EtlStepList


case class EtlJob1Props (
        ratings_output_table_name: String = "ratings",
) extends EtlJobProps

case class Job1SparkS3andGCSandBQSteps(job_properties: EtlJob1Props, globalProperties: Config) extends SequentialEtlJob {
      
val step1 = HttpStep(
             name         = "HttpPostJson",
             url          = "https://httpbin.org/post",
             http_method  = HttpMethod.POST,
             params       = Left("""{"key":"value"}"""),
             headers      = Map("content-type"->"application/json"),
             log_response = true,
)
      
val step2 = HttpStep(
                   name         = "HttpPostJson",
                   url          = "https://httpbin.org/post",
                   http_method  = HttpMethod.POST,
                   params       = Left("""{"key":"value"}"""),
                   headers      = Map("content-type"->"application/json"),
                   log_response = true,
)
            
                
val parstep = ParallelETLStep("ParallelStep")(step1,step2)
      
val etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(parstep)

}
```
              
