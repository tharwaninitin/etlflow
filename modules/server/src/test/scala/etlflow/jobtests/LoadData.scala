package etlflow.jobtests

import etlflow.etljobs.EtlJob
import etlflow.{CliApp, EtlJobProps}

object LoadData extends CliApp[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]
