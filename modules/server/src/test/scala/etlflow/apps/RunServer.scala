package etlflow.apps

import etlflow.{EtlJobProps, MyEtlJobName, ServerApp}

object RunServer extends ServerApp[MyEtlJobName[EtlJobProps], EtlJobProps]
