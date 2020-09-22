package etlflow.executor

import caliban.CalibanError.ExecutionError
import etlflow.{EtlJobName, EtlJobProps}
import etlflow.scheduler.api.EtlFlowHelper.{EtlJob, EtlJobArgs}
import etlflow.utils.JsonJackson
import zio.Task
import etlflow.utils.{UtilityFunctions => UF}

trait EtlJobValidator {

  final def validateJob(args: EtlJobArgs, etl_job_name_package: String): Task[EtlJob] = {
    val etlJobDetails: Task[(EtlJobName[EtlJobProps], Map[String, String])] = Task {
      val job_name = UF.getEtlJobName[EtlJobName[EtlJobProps]](args.name, etl_job_name_package)
      val props_map = args.props.map(x => (x.key, x.value)).toMap
      (job_name, props_map)
    }.mapError { e =>
      println(e.getMessage)
      ExecutionError(e.getMessage)
    }
    for {
      (job_name, props_map) <- etlJobDetails
      execution_props <- Task {
        JsonJackson.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
          .map(x => (x._1, x._2.toString))
      }.mapError { e =>
        println(e.getMessage)
        ExecutionError(e.getMessage)
      }
    } yield EtlJob(args.name, execution_props)
  }

}
