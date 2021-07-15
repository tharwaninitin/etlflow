---
layout: docs
title: Parallel Step Execution
---

## Http Step

**This page shows how to execute more than one steps in parallel**

### Example 1
Below is the sample Http Step example. 

```scala mdoc
        
import etlflow.etlsteps._
import etlflow.etlsteps.ParallelETLStep
import etlflow.etljobs.SequentialEtlJob
import etlflow.EtlJobProps
import etlflow.EtlStepList
import etlflow.utils.HttpMethod

case class EtlJob1Props (
        ratings_output_table_name: String = "ratings",
) extends EtlJobProps

case class Job1SparkS3andGCSandBQSteps(job_properties: EtlJob1Props) extends SequentialEtlJob[EtlJob1Props] {
      
  val postStep1 = HttpRequestStep[Unit](
    name         = "HttpPostJson",
    url          = "https://httpbin.org/post",
    method       = HttpMethod.POST,
    params       = Left("""{"key":"value"}"""),
    headers      = Map("X-Auth-Token"->"abcd.xxx.123"),
    log          = true,
  )

  val postStep2 = HttpRequestStep[Unit](
    name         = "HttpPostForm",
    url          = "https://httpbin.org/post?signup=yes",
    method       = HttpMethod.POST,
    params       = Right(Map("name" -> "John", "surname" -> "doe")),
    log          = true,
  )
            
                
val parstep = ParallelETLStep("ParallelStep")(postStep1,postStep2)
      
val etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(parstep)

}
```
              
