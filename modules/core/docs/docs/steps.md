---
layout: docs
title: EtlSteps
---

## Steps

**Step** is fundamental unit for describing task in EtlFlow.
Steps are atomic, meaning they can run on their own or together inside **Job**.
Following is the list of steps available in this library:
- SparkReadWriteStep
- SparkReadTransformWriteStep
- BQLoadStep
- BQQueryStep
- S3PutStep
- GCSPutStep
- DBQueryStep