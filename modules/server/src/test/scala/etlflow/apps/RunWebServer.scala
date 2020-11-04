package etlflow.apps

import etlflow.{EtlJobProps, MyEtlJobName}
import etlflow.webserver.WebServerApp

object RunWebServer extends WebServerApp[MyEtlJobName[EtlJobProps], EtlJobProps]
