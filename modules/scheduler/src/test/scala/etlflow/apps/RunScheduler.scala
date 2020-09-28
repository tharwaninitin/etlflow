package etlflow.apps

import etlflow.{EtlJobProps, MyEtlJobName}
import etlflow.scheduler.SchedulerApp

object RunScheduler extends SchedulerApp[MyEtlJobName[EtlJobProps], EtlJobProps]
