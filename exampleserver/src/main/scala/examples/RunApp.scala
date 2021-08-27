package examples

import etlflow.etljobs.EtlJob
import etlflow.{EtlJobProps, ServerApp}
import examples.schema.MyEtlJobPropsMapping

object RunApp extends ServerApp[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]
