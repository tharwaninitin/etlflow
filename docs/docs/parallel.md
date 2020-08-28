---
layout: docs
title: Parallel Step Execution
---

## Http Step

**This page shows how to execute more than one steps in parallel**

### Example 1
Below is the sample Http Step GET example. 

      val step1 = HttpStep(
             name         = "HttpPostJson",
             url          = "https://httpbin.org/post",
             http_method  = HttpMethod.POST,
             params       = Left("""{"key":"value"}"""),
             headers      = Map("content-type"->"application/json"),
             log_response = true,
           )
      
      val step2 = HttpResponseStep(
              name         = "HttpGetParams",
              url          = "https://httpbin.org/get",
              http_method  = HttpMethod.GET,
              params       = Right(Seq(("param1","value1"))),
              log_response = true,
            ) 
                
      val parstep = ParallelETLStep("ParallelStep")(step1,step2)
      
      val etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(parstep) 

              
