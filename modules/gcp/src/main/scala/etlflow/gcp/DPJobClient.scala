package etlflow.gcp

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.dataproc.v1.{JobControllerClient, JobControllerSettings}
import etlflow.model.Credential.GCP
import java.io.FileInputStream

object DPJobClient {

  private def getDPJob(path: String, endpoint: String): JobControllerClient = {
    val credentials: GoogleCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(path))
    val jcs = JobControllerSettings
      .newBuilder()
      .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
      .setEndpoint(endpoint)
      .build()
    JobControllerClient.create(jcs)
  }

  def apply(endpoint: String, credentials: Option[GCP]): JobControllerClient = {
    val env_path: String = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", "NOT_SET_IN_ENV")

    credentials match {
      case Some(creds) =>
        logger.info("Using GCP credentials from values passed in function")
        getDPJob(creds.service_account_key_path, endpoint)
      case None =>
        if (env_path == "NOT_SET_IN_ENV") {
          logger.info("Using GCP credentials from local sdk")
          val jcs = JobControllerSettings.newBuilder().setEndpoint(endpoint).build()
          JobControllerClient.create(jcs)
        } else {
          logger.info("Using GCP credentials from environment variable GOOGLE_APPLICATION_CREDENTIALS")
          getDPJob(env_path, endpoint)
        }
    }
  }
}
