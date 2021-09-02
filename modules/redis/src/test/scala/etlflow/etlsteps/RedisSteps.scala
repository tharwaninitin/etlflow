package etlflow.etlsteps

import etlflow.EtlStepList
import etlflow.coretests.Schema.EtlJob3Props
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.RedisStep.RedisCmd
import etlflow.schema.Credential.REDIS

case class RedisJob(job_properties: EtlJob3Props) extends SequentialEtlJob[EtlJob3Props] {

  val redis_config: REDIS = REDIS("localhost")

  val step1 = RedisStep(
    name         = "set_redis_key_value_1",
    command      = RedisCmd.SET(Map("key1" -> "value1","key2" -> "value3","key3" -> "value3")),
    credentials  = redis_config
  )

  val step2 = RedisStep(
    name         = "set_redis_key_value_2",
    command      = RedisCmd.SET(Map("key4" -> "value4","key5" -> "value5","key6" -> "value6")),
    credentials  = redis_config
  )

  val step12 = ParallelETLStep("redis_parallel_inserts")(step1,step2)

  val step3 = RedisStep(
    name        = "delete_keys_from_redis",
    command     = RedisCmd.DELETE(List("*key1*")),
    credentials = redis_config
  )


  val step4 = RedisStep(
    name        = "flushall_keys_from_redis",
    command     = RedisCmd.FLUSHALL,
    credentials = redis_config
  )

  val step5 = RedisStep(
    name        = "delete_none_from_redis",
    command     = RedisCmd.DELETE(List("*key1*")),
    credentials = redis_config
  )

  override def etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step12,step3,step4, step5)
}

