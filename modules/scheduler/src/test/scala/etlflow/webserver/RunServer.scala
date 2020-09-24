package etlflow.webserver

import etlflow.EtlJobProps
import etlflow.scheduler.MyEtlJobName

object RunServer extends WebServer[MyEtlJobName[EtlJobProps], EtlJobProps]
