package etlflow.apps

import etlflow.{EtlJobProps, ServerApp}
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.etljobs.EtlJob

object RunServer extends ServerApp[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]
