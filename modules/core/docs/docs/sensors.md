---
layout: docs
title: Sensors
---

## Sensors

As **Step** can perform any process that can be executed in Scala. Similarly, **Sensors** can check the state of any process or data structure.
**They can be used to pause the execution of dependent steps until some criterion has been met**. Basically they are steps with additional retry capabilities.

Following is the list of sensors available in this library:
- GCSSensorStep
- S3SensorStep
- JDBCSensorStep