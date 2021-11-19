package etlflow.apps

import etlflow.{EtlJobProps, ServerApp}
import etlflow.etljobs.EtlJob
import etlflow.jobtests.MyEtlJobPropsMapping

object RunServer extends ServerApp[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]
