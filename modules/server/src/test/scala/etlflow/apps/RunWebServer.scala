package etlflow.apps

import etlflow.EtlJobProps
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.etljobs.EtlJob
import etlflow.webserver.WebServerApp

object RunWebServer extends WebServerApp[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]
