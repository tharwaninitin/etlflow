package etlflow.utils

import etlflow.utils.{UtilityFunctions => UF}
import org.scalatest.{FlatSpec, Matchers}
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => ru}

class ReflectionTestSuite extends FlatSpec with Matchers {

  sealed trait EtlJob
  sealed trait EtlJobName

  case class Job1() extends EtlJobName
  case class Job2() extends EtlJobName
  case class Job3() extends EtlJobName with EtlJob
  case class Job4() extends EtlJobName with EtlJob
  case object Job5 extends EtlJobName

  def getSubClasses1[T: TypeTag]: Set[String] = {
    val tpe = ru.typeOf[T]
    val clazz = tpe.typeSymbol.asClass
    val allJobNames = clazz.knownDirectSubclasses
    allJobNames.map(x => x.name.toString)
  }

  def getSubClasses2[T: TypeTag]: List[String] = {
    val tpe = ru.typeOf[T]
    val allJobNames = tpe.baseClasses
    allJobNames.map(x => x.name.toString)
  }
   println(getSubClasses1[EtlJobName])
   println(getSubClasses2[EtlJobName])
   println(getSubClasses2[Job4])

  "getEtlJobs(EtlJobName) should " should "retrieve Set successfully" in {
    assert(UF.getEtlJobs[EtlJobName] == Set("Job1", "Job2", "Job3", "Job4", "Job5"))
  }

  "getEtlJobs(EtlJob) should " should "retrieve Set successfully" in {
    assert(UF.getEtlJobs[EtlJob] == Set("Job3", "Job4"))
  }
}
