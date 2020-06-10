FROM jupyter/all-spark-notebook:latest

USER $NB_UID

RUN conda install -c conda-forge --quiet --yes \
    'jupyter_contrib_nbextensions' \
    'jupyterthemes' && \
    jt -t chesterish -cellw 100%
