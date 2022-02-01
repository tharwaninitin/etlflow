package etlflow.gcp

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.dataproc.v1.{ClusterControllerClient, ClusterControllerSettings}
import etlflow.model.Credential.GCP
import java.io.FileInputStream

object DPClient {

  private def getDP(path: String, endpoint: String): ClusterControllerClient = {
    val credentials: GoogleCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(path))
    val ccs = ClusterControllerSettings.newBuilder
      .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
      .setEndpoint(endpoint)
      .build
    ClusterControllerClient.create(ccs)
  }

  def apply(endpoint: String, credentials: Option[GCP]): ClusterControllerClient = {
    val env_path: String = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", "NOT_SET_IN_ENV")

    credentials match {
      case Some(creds) =>
        logger.info("Using GCP credentials from values passed in function")
        getDP(creds.service_account_key_path, endpoint)
      case None =>
        if (env_path == "NOT_SET_IN_ENV") {
          logger.info("Using GCP credentials from local sdk")
          val ccs = ClusterControllerSettings.newBuilder.setEndpoint(endpoint).build
          ClusterControllerClient.create(ccs)
        } else {
          logger.info("Using GCP credentials from environment variable GOOGLE_APPLICATION_CREDENTIALS")
          getDP(env_path, endpoint)
        }
    }
  }
}
