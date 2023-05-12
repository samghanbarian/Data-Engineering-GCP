to work with spark we need to download jdk version 8 or 11. we download from folloing link openjdk
 https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz

 ```bash
 mkdir spark
 cd spark
 wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
 tar xzvf openjdk-11.0.2_linux-x64_bin.tar.gz
 ```

 add the path to jdk to .bashrc
 ```bash
 export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
 export PATH="${JAVA_HOME}/bin:${PATH}"
 ```

 install spark 3.3 stable release prebuilt on apache hadoop 3.2
 ```bash
 wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar xzfv spark-3.3.2-bin-hadoop3.tgz
rm spark-3.3.2-bin-hadoop3.tgz
```

add the path to executable to .bashrc
```bash
export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
```

to test the spark installation
```bash
spark-shell
```
```java
val data = 1 to 1000
val distData = sc.parallelized(data)
distData.filter(_ < 10).collect()
```
add pythonpath to run pyspark
```bash
export PYTHONPATH="${SPARK_HOME}/python:${PYTHONPATH}"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:${PYTHONPATH}"
```


