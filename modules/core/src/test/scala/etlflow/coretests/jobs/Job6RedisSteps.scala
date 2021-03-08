package etlflow.coretests.jobs

import etlflow.EtlStepList
import etlflow.coretests.Schema.EtlJob3Props
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{EtlStep, RedisStep}
import etlflow.utils.REDIS
import etlflow.etlsteps.RedisStep.RedisCmd

case class Job6RedisSteps(job_properties: EtlJob3Props) extends SequentialEtlJob[EtlJob3Props] {

  val redis_config: REDIS = REDIS("localhost")

  val step1 = RedisStep(
    name         = "Set redis key and value",
    command      = RedisCmd.SET(Map("key1" -> "value1","key2" -> "value3","key3" -> "value3")),
    credentials  = redis_config
  )

  val step2 = RedisStep(
    name        = "delete the keys from redis",
    command     = RedisCmd.DELETE(List("*key1*")),
    credentials = redis_config
  )

  val step3 = RedisStep(
    name        = "flushall the keys from redis",
    command     = RedisCmd.FLUSHALL,
    credentials = redis_config
  )

  override def etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1,step2,step3)
}

