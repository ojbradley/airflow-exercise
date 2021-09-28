# A simple Airflow DAG that uses the SparkSubmitOperator to load, cleanse, merge, and write out files from 2x directories

#### In order to run this DAG, ensure you have covered the following pre-requisites first

    * Apache Airflow is installed on your machine
    * Pyspark is installed on your machine
    * SPARK_HOME is configured as an environment variable and added to your PATH
    * All folders from this repo have been copied to the root of your AIRFLOW_HOME directory after cloning
    * A global variable called "PYSPARK_APP_HOME" has been created in the Airflow GUI that points to the /dags/scripts directory you set up in step 4

### Installing Airflow Locally

    * To install Airflow locally, follow the steps highlighted [here](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html)

### Installing Pyspark Locally
    * Installing Pyspark locally is a more involved process. This is widely documented online, but I have found [this tutorial](https://maelfabien.github.io/bigdata/SparkInstall/#step-5-install-pyspark) quick and relatively painless.

### Assumptions Made In The Application
    * Input_source_1 will only ever contain json files
    * Input_source_2 will only ever contain csv files
    * Both Input_Source folders must contain at least 1x file with the expected extensions in order for the Spark app to run