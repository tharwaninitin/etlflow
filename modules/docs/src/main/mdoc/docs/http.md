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
import etlflow.utils.HttpRequest.HttpMethod

val step1 = HttpRequestStep[Unit](
    name    = "HttpGetSimple",
    url     = "https://httpbin.org/get",
    method  = HttpMethod.GET,
    log     = true,
    connection_timeout = 1200000
 )
```       
### Example 2
We can use below http GET step when we want return response from the step. 

```scala mdoc
import etlflow.etlsteps._
import etlflow.utils.HttpRequest.HttpMethod

val step2 = HttpRequestStep[String](
    name         = "HttpGetParams",
    url          = "https://httpbin.org/get",
    method       = HttpMethod.GET,
    params       = Right(Map("param1"-> "value1")),
    log          = true,
)
```      
 
### Example 3
Below is the simple Http POST json  example. 

```scala mdoc
import etlflow.etlsteps._
import etlflow.utils.HttpRequest.HttpMethod

val step3 = HttpRequestStep[Unit](
    name         = "HttpPostJson",
    url          = "https://httpbin.org/post",
    method       = HttpMethod.POST,
    params       = Left("""{"key":"value"}"""),
    headers      = Map("X-Auth-Token"->"abcd.xxx.123"),
    log          = true,
)
```
      
### Example 4
We can use below http POST step when we want return response from the step. 

```scala mdoc
import etlflow.etlsteps._
import etlflow.utils.HttpRequest.HttpMethod

val step4 = HttpRequestStep[String](
    name         = "HttpPostForm",
    url          = "https://httpbin.org/post?signup=yes",
    method       = HttpMethod.POST,
    params       = Right(Map("name" -> "John", "surname" -> "doe")),
    log          = true,
)
```              
