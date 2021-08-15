package etlflow.utils

import etlflow.EtlJobProps
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.etljobs.EtlJob

object ReflectionHelper {

  type MEJP = MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]

  sealed trait EtlJobTest

  object EtlJobTest {
    case object Job1 extends EtlJobTest
    case object Job2 extends EtlJobTest
  }

  sealed trait EtlJobName

  object EtlJobName {
    case class Job1(prop1: String, prop2: Int) extends EtlJobName
    case class Job2() extends EtlJobName
    case class Job3() extends EtlJobName
    case class Job4() extends EtlJobName
    case class Job5() extends EtlJobName
  }
}
