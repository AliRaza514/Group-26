# Set critical environment variables
$env:HADOOP_HOME = "C:\Program Files\spark-3.5.5-bin-hadoop3"
$env:SPARK_DIST_CLASSPATH = "$env:HADOOP_HOME\jars\*"

# Create dummy Hadoop files to bypass Windows checks
New-Item -ItemType File -Force -Path "$env:HADOOP_HOME\bin\hadoop.dll"
New-Item -ItemType File -Force -Path "$env:HADOOP_HOME\bin\winutils.exe"

# Run Spark with all security disabled
spark-submit `
    --conf "spark.hadoop.fs.defaultFS=file:///" `
    --conf "spark.driver.extraJavaOptions=-Dhadoop.security.authentication=simple -Djava.security.auth.login.config=DISABLED" `
    --conf "spark.executor.extraJavaOptions=-Dhadoop.security.authentication=simple" `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 `
    now_trending.py