---
layout: docs
title: Send Mail
---

## Send Mail Step

**This page shows Send Mail Step available in this library**

## Parameters
* **name** [String] - Description of the Step.
* **body** [String] - Email body. 
* **subject** [String] - Subject of the email.
* **recipient_list** [List(String)] - Recipient list.
* **credentials** [SMTP] - smtp credentials

### Example 1
Below is the sample example for send mail step 

```scala mdoc

import etlflow.etlsteps._
import etlflow.Credential.SMTP
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

val emailBody: String = {
         val exec_time = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)
         s"""
            | SMTP Email Test
            | Time of Execution: $exec_time
            |""".stripMargin
}
      
val step = SendMailStep(
        name           = "SendSMTPEmail",
        body           = emailBody,
        subject        = "EtlFlow Ran Successfully",
        recipient_list = List("abc@<domain>.com"),
        credentials    = SMTP("PORT", "HOST", "USER", "PWD")
)
```