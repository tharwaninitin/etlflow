package examples

import etlflow.etljobs.EtlJob
import etlflow.{EtlFlowApp, EtlJobProps}
import examples.schema.MyEtlJobPropsMapping


object LoadData extends EtlFlowApp[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]
