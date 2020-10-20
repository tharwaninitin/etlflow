package examples

import etlflow.{EtlFlowApp, EtlJobProps}
import examples.schema.MyEtlJobName


object LoadData extends EtlFlowApp[MyEtlJobName[EtlJobProps], EtlJobProps]
