---
layout: docs
title: Core Installation
---

## Http Step

**This page shows different Http Step (Post/Get) available in this library**

Below are the supported steps : 
* HttpStep => **When we want to just callback the url. No response return**
* HttpResponseStep  => **When we want to http response from the step**

## Parameters
* **name** [String] - Description of the Step.
* **url** [String] - Specify valid URL . 
* **http_method** [HttpMethod] - Http method to be used (GET/POST).
* **params** [Either(String, Seq((String,String)))] - Provide params if want to send some contents in get or post request(Optional).
* **headers** [Map[String,String]] - Provide header if any (Optional).
* **log_response** [Boolean] - This param will print the logs when provideded value is true. Default is false.

### Example 1
Below is the sample Http Step GET example. 

```scala mdoc
import etlflow.etlsteps._

val step1 = HttpStep(
         name         = "HttpGet",
         url          = "https://httpbin.org/get",
         http_method  = HttpMethod.GET,
         log_response = true,
)
```       
### Example 2
We can use below http GET step when we want return response from the step. 

```scala mdoc
import etlflow.etlsteps._

val step2 = HttpResponseStep(
        name         = "HttpGetParams",
        url          = "https://httpbin.org/get",
        http_method  = HttpMethod.GET,
        params       = Right(Seq(("param1","value1"))),
        log_response = true,
)
```      
 
### Example 3
Below is the simple Http POST json  example. 

```scala mdoc
import etlflow.etlsteps._

val step3 = HttpStep(
        name         = "HttpPostJson",
        url          = "https://httpbin.org/post",
        http_method  = HttpMethod.POST,
        params       = Left("""{"key":"value"}"""),
        headers      = Map("content-type"->"application/json"),
        log_response = true,
)
```
      
### Example 4
We can use below http POST step when we want return response from the step. 

```scala mdoc
import etlflow.etlsteps._

val step4 = HttpResponseStep(
         name         = "HttpPostParams",
         url          = "https://httpbin.org/post",
         http_method  = HttpMethod.POST,
         params       = Right(Seq(("param1","value1"))),
         log_response = true,
)
```              
