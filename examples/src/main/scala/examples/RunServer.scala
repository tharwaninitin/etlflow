package examples

import etlflow.{EtlJobProps, ServerApp}
import examples.schema.MyEtlJobName

object RunServer extends ServerApp[MyEtlJobName[EtlJobProps], EtlJobProps]
