package etlflow.jobs

import etlflow.EtlStepList
import etlflow.Schema.EtlJob3Props
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{EtlStep, RedisStep}
import etlflow.utils.REDIS

case class Job6RedisJobSteps(job_properties: EtlJob3Props) extends SequentialEtlJob[EtlJob3Props] {

  val redis_config = REDIS("localhost")

  val step1 = RedisStep(
    name                    = "Set redis key and value",
    operation_type          = "set",
    kv                      = Map("key1" -> "value1","key2" -> "value3","key3" -> "value3"),
    credentials             = redis_config
  )

  val step2 = RedisStep(
    name                    = "delete the keys from redis",
    operation_type          = "delete",
    prefix                  = List("*key1*"),
    credentials             = redis_config
  )

  val step3 = RedisStep(
    name                    = "flushall the keys from redis",
    operation_type          = "flushall",
    credentials             = redis_config
  )

  override def etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1,step2,step3)
}
