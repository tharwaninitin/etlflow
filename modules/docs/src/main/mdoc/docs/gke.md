---
layout: docs
title: Gke Server Installation
---

## Using Etlflow Server library on gke environment with deployment yaml files.

### Prerequisite
* Your system should have [Kubernetes Engine](https://cloud.google.com/kubernetes-engine) installed.
* You should have GCP service account access with appropriate permissions for GCS, BigQuery, Submitting and deploying jobs etc.

### STEP 1) Building docker image for examples project.
* Update the file **examples/src/main/resources/application.conf** with below content : 
        
        
        db-log = {
                  url = <log_db_url>,
                  user = <log_db_user>,
                  password = <log_db_pwd>,
                  driver = "org.postgresql.Driver"
                }
        slack = {
                  url = <slack-url>,
                  env = <running-env>
                }

* Copy GCP service account json file at location **examples/src/main/conf** with name **cred.json**

* To build docker image we have to run below commands. 

         
         > sbt
         > project examples
         > docker:publishLocal

### STEP 2) Now we have successfully built an image. Let's deploy this image on google kubernetes engine. 

GKE contains the 2 types of yaml files : 
* deployment.yaml: Provide a configuration to run the docker image.
    
```
       apiVersion: apps/v1
       kind: Deployment
       metadata:
         name: etlflow-gke
         namespace: <NAME-SPAECE>
       spec:
         replicas: 1
         selector:
           matchLabels:
             app: etlflow-gke
         template:
           metadata:
             labels:
               app: etlflow-gke
           spec:
             containers:
             - name: etlflow-gke
               # Replace $GCLOUD_PROJECT with your project ID
               image: gcr.io/<GCLOUD_PROJECT>/etlflow:0.1.0
               # This app listens on port 8080 for web traffic by default.
               ports:
               - containerPort: 8080
               env:
                 - name: PORT
                   value: "8080"
 ```    

* service.yaml: Provide a configuration to access the deployed application from front-end.
      
      
      apiVersion: v1
      kind: Service
      metadata:
        name: etlflow-gke
      spec:
        type: LoadBalancer
        selector:
          app: etlflow-gke
        ports:
        - port: 80
          targetPort: 8080


* Use below commands to run/stop deployed the etlflow on GKE  using above two configuration files : 

      
      kubectl apply -f deployment.yaml ( Deploy the resource to the cluster )
      kubectl get deployments ( Track the status of the Deployment )    
      kubectl get pods ( To check the pods that the Deployment created)
      kubectl apply -f service.yaml ( To Create the service )
      kubectl get services ( Get the Service's external IP address )  
     

[To setup a kubernetes cluster](https://github.com/tharwaninitin/etlflow/tree/feature11/deployment/gcp)