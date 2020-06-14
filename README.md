# Time series prediction (scala)

Predict the sales of different items for a time period of 28 days. 
The data to which the model is applied is from the 
[M5 Forecasting Kaggle competition](https://www.kaggle.com/c/m5-forecasting-accuracy).
  
## Setup 

### Setup Jupyter environment (Jupyter Docker Stacks image)     
- For model prototyping we use a Jupyter notebook from 
[Jupyter Docker Stacks](https://hub.docker.com/r/jupyter/all-spark-notebook/).
The `Dockerfile` is build on top of this image and just enables the Jupyter notebook 
extensions. 
    ```bash
    # build the image
    docker build -t ai-all-spark:0.1 .
    
    # start a container (after setting up data_dir)
    docker run -it --name sharky \
        -p 8889:8888 \
        -v "$(pwd)":/home/jovyan/work \
        -v "$DATA_DIR":/home/jovyan/work/data \
        ai-all-spark:0.1
    ```

- Import `.jar` files in a spark session in a Jupyter notebook.   
    When an `Apache Toree - Scala` notebook is opened a spark session is
    automatically initialized. In order to import a jar-file you have to
    modify the kernel startup script: 
    ```bash
    # Access the container
    docker exec -it sharky /bin/bash
  
    # Show the folders where the essential information about the Jupyter kernels is stored
    jupyter kernelspec list
    
    # Go the directory where the apache_toree_scala is located
    cd /opt/conda/share/jupyter/kernels/apache_toree_scala
  
    # from "argv" you should be able to locate the kernel startup script
    # /opt/conda/share/jupyter/kernels/apache_toree_scala/bin/run.sh  
    cat kernel.json
    ``` 
    Now, you can modify the shell script by adding the jars of interest. In particular, 
    we modify
    ```text
    eval exec \
         "${SPARK_HOME}/bin/spark-submit" \
         --name "'Apache Toree'" \
         "${SPARK_OPTS}" \
         --class org.apache.toree.Main \
         "${TOREE_ASSEMBLY}" \
         "${TOREE_OPTS}" \
         "$@"
    ```
    to
    ```text
    eval exec \
         "${SPARK_HOME}/bin/spark-submit" \
         --name "'Apache Toree'" \
         "${SPARK_OPTS}" \
         --class org.apache.toree.Main \
         --jars < location of the jar file > \
         "${TOREE_ASSEMBLY}" \
         "${TOREE_OPTS}" \
         "$@"
    ```
    Exit from the container with `^Q ^P` (not sure).
    
    You can check if the new jars are present in the spark session by opening a 
    Jupyter notebook and executing `spark.sparkContext.listJars.foreach(println)`
    
### Setup Jupyter environment (Microsoft MMLSpark image)
- Just follow the instructions from the official [Github repository](https://github.com/Azure/mmlspark/tree/master)
    ```bash
    docker run -it --rm \
       -p 127.0.0.1:80:8888 \
       -v "$(pwd)":/notebooks/myfiles \
       -v "$DATA_DIR":/notebooks/data \
       mcr.microsoft.com/mmlspark/release:latest
    ```

### Package
- Create `.jar`   
    ```bash
    sbt assembly
    ```
- Test  
    At the moment, only several functions are tested.
    ```bash
    sbt test
    ```
