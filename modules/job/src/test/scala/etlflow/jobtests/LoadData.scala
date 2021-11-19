package etlflow.jobtests

import etlflow.etljobs.EtlJob
import etlflow.{EtlFlowApp, EtlJobProps}

object LoadData extends EtlFlowApp[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]
