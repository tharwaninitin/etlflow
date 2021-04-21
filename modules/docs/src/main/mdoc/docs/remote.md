---
layout: docs
title: Remote Steps
---

## Remote Steps

**This page shows different Remote Steps available in this library**

### EtlFlowJobStep : 
We can use below step when we want to submit child job from parent job.


* Child Job 

```scala mdoc
import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{EtlStep, GenericETLStep}
import etlflow.EtlJobProps

case class EtlJob4Props (
  ratings_input_path: String = "input_path",
) extends EtlJobProps

case class HelloWorldJob(job_properties: EtlJob4Props) extends SequentialEtlJob[EtlJob4Props] {

  def processData(ip: Unit): Unit = {
    logger.info("Hello World")
  }

  val step1 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  override def etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1)
}

```
 
* Parent Job 
 
```scala mdoc
import etlflow.etlsteps.EtlFlowJobStep
      
val step = EtlFlowJobStep[EtlJob4Props](
          name = "Test",
          job  = HelloWorldJob(EtlJob4Props()),
    )
```
