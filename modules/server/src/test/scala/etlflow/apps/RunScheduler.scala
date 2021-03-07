package etlflow.apps

import etlflow.EtlJobProps
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.etljobs.EtlJob
import etlflow.scheduler.SchedulerApp

object RunScheduler extends SchedulerApp[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]
