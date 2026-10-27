```bat
@echo off
title Social Media Trend Detection - Startup

set "PROJECT=C:\Users\ashiq\Documents\social-trend-project"
set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot"
set "HADOOP_HOME=C:\hadoop"
set "PATH=C:\hadoop\bin;%JAVA_HOME%\bin;%PATH%"

echo Starting Kafka...
start "Kafka Server" cmd /k "cd /d C:\kafka && set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G && .\bin\windows\kafka-server-start.bat .\config\server.properties"

timeout /t 15 /nobreak >nul

echo Starting Social Media Producer...
start "Kafka Producer" cmd /k "cd /d %PROJECT% && .venv\Scripts\python.exe data_generator.py"

echo Starting Streamlit Dashboard...
start "Dashboard" cmd /k "cd /d %PROJECT% && .venv\Scripts\python.exe -m streamlit run dashboard.py"

echo Starting Spark Streaming...
start "Spark Streaming" cmd /k "cd /d %PROJECT% && set JAVA_HOME=%JAVA_HOME% && set HADOOP_HOME=%HADOOP_HOME% && set PATH=C:\hadoop\bin;%JAVA_HOME%\bin;%PATH% && set PYSPARK_SUBMIT_ARGS=--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 pyspark-shell && .venv-spark\Scripts\python.exe spark_streaming.py"

echo.
echo All startup commands launched.
echo Dashboard: http://localhost:8501
pause
```