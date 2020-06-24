package etlflow.etlsteps

import etlflow.utils.{DataprocHelper, GlobalProperties}
import zio.Task

class DataprocSparkJobStep(
                                   val name:String  ,
                                   val job_name: String,
                                   val props: Map[String,String],
                                   val global_properties: Option[GlobalProperties] = None
                                 )
  extends EtlStep[Unit,Unit] with DataprocHelper {

  override val gcp_region: String = global_properties.map(x => x.gcp_region).get
  override val gcp_project: String = global_properties.map(x => x.gcp_project).get
  override val gcp_dp_endpoint: String = global_properties.map(x => x.gcp_dp_endpoint).get
  override val gcp_dp_cluster_name: String = global_properties.map(x => x.gcp_dp_cluster_name).get
  override val main_class: String = global_properties.map(x => x.main_class).get
  override val dp_libs: List[String] = global_properties.map(x => x.dep_libs).get.split(",").toList

  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting Job Submission for : $job_name")
    executeDataProcJob(job_name,props)
  }

  override def getStepProperties(level: String): Map[String, String] = Map("query" -> job_name)
}


object DataprocSparkJobStep {
  def apply( name:String,
             job_name: String,
             props: Map[String,String],
             global_properties: Option[GlobalProperties] = None): DataprocSparkJobStep =
    new DataprocSparkJobStep(name,job_name,props,global_properties)
}


