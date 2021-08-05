## For testing GCPDataprocCreateTestSuite and GCPDataprocDeleteTestSuite follow below instructions
	export GOOGLE_APPLICATION_CREDENTIALS=<...> #this should be full path to GCP Service Account Key Json which should have PUBSUB Read access 
	export DP_PROJECT_ID=<...>
    export DP_REGION=<...>
    export DP_ENDPOINT=<...>
    export BUCKET_NAME=<...>
    export IMAGE_VERSION=<...>
    export BOOT_DISK_TYPE=<...>
    export MASTER_BOOT_DISK_SIZE=<...>
    export WORKER_BOOT_DISK_SIZE=<...>
    export SUBNET_WORK_URI=<...>
    export ALL_TAGS=<...>
    export MASTER_MACHINE_TYPE_URI=<...>
    export WORKER_MACHINE_TYPE_URI=<...>
    export MASTER_NUM_INSTANCE=<...>
    export WORKER_NUM_INSTANCE=<...>
	
	sbt "project cloud" testOnly etlflow.steps.cloud.GCPDataprocCreateTestSuite
	sbt "project cloud" testOnly etlflow.steps.cloud.GCPDataprocDeleteTestSuite