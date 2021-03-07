package etlflow.executor

import caliban.CalibanError.ExecutionError
import etlflow.{EtlJobPropsMapping, EtlJobProps}
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.utils.EtlFlowHelper.{EtlJob, EtlJobArgs}
import etlflow.utils.{JsonJackson, UtilityFunctions => UF}
import zio.Task

trait EtlJobValidator {

  final def validateJob(args: EtlJobArgs, etl_job_name_package: String): Task[EtlJob] = {
    val etlJobDetails: Task[(EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]], Map[String, String])] = Task {
      val job_name = UF.getEtlJobName[EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]]](args.name, etl_job_name_package)
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
