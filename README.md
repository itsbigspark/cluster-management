# bigspark - Cloudera Management suite
###Compilation 

`mvn package`

Will output 2 jars:  Main artifact and -tests.jar

###Package structure
All cluster management jobs have been created in a consistent way and comprise of the following structure:

_com.bigspark.cloudera.management.jobs   
| --> Job     
| -- | -->  JobRunner    
| -- | -->  JobMistRunner    
| -- | -->  JobController    
| -- | -->  Job_     


JobRunner - Entry point class for the job type, used to accept input parameters and invoke the controller
JobMistRunner - Wrapper class to execute the job via the Mist framework
JobController - This is used to generate the the workload for the respecitve job and contains all the logic for inclusion/exclusion of processing candidates
Job - The actual job process with operates at some atmoic grain - i.e.  Table or Location

###Testing 
Each job type has a corresponding Test runner within the testing jar
Please refer to each MD file in the respective job package for specific notes on integration testing

###Testing / Execution notes

1. In order to avoid java dependency issues    
1a. Ideally use a version of Spark without a compiled Hadoop version.  Even better would be a cloudera Spark version    
1b. Submit spark jobs in cluster mode (--deploy-mode cluster)     
3. To execute integration testing jobs from the testing jar, you must also attach the cluster management jar as a --jars arg     



