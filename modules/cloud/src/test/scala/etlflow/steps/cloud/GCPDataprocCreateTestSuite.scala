package etlflow.steps.cloud

import etlflow.etlsteps.{DPCreateStep, DPDeleteStep, DPHiveJobStep, DPSparkJobStep}
import etlflow.utils.Executor.DATAPROC
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object GCPDataprocCreateTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Steps") (
      testM("Execute DPCreateStep") {

        val create_cluster_spd_props: Map[String, String] =
          Map(
            "project_id" -> sys.env("DP_PROJECT_ID"),
            "region" -> sys.env("DP_REGION"),
            "endpoint"-> sys.env("DP_ENDPOINT"),
            "bucket_name"-> sys.env("BUCKET_NAME"),
            "image_version"->sys.env("IMAGE_VERSION"),
            "boot_disk_type"->sys.env("BOOT_DISK_TYPE"),
            "master_boot_disk_size"->sys.env("MASTER_BOOT_DISK_SIZE"),
            "worker_boot_disk_size"->sys.env("WORKER_BOOT_DISK_SIZE"),
            "subnet_work_uri"->sys.env("SUBNET_WORK_URI"),
            "all_tags"->sys.env("ALL_TAGS"),
            "master_machine_type_uri"->sys.env("MASTER_MACHINE_TYPE_URI"),
            "worker_machine_type_uri"->sys.env("WORKER_MACHINE_TYPE_URI"),
            "master_num_instance" ->sys.env("MASTER_NUM_INSTANCE"),
            "worker_num_instance" ->sys.env("WORKER_NUM_INSTANCE")
          )

        val step = DPCreateStep(
          name                    = "DPCreateStepExample",
          cluster_name            = "test",
          props                   = create_cluster_spd_props
        )
        assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }

    )
}
