package etlflow.executor

import caliban.CalibanError.ExecutionError
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.log.ApplicationLogger
import etlflow.utils.EtlFlowHelper.{EtlJob, EtlJobArgs}
import etlflow.utils.{JsonJackson, UtilityFunctions => UF}
import etlflow.{EtlJobProps, EtlJobPropsMapping}
import zio.IO

trait EtlJobValidator extends ApplicationLogger {
  final def validateJob(args: EtlJobArgs, etl_job_name_package: String): IO[ExecutionError,EtlJob] = IO {
    val job_name = UF.getEtlJobName[EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]]](args.name, etl_job_name_package)
    val props_map = args.props.map(x => (x.key, x.value)).toMap
    val execution_props = JsonJackson.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty).map(x => (x._1, x._2.toString))
    EtlJob(args.name, execution_props)
  }.mapError { e =>
    logger.error("Job validation failed with " + e.getMessage)
    ExecutionError("Job validation failed with " + e.getMessage)
  }
}
