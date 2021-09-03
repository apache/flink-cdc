FROM python:3.7-slim
RUN apt-get update
RUN apt-get -y install git
RUN pip3 install -U sphinx==4.1.1 myst-parser pygments sphinx-rtd-theme sphinx-autobuild gitpython
EXPOSE 8001
CMD ["sphinx-autobuild", "--host", "0.0.0.0", "--port", "8001", "/home/flink-cdc/docs", "/home/flink-cdc/docs/_build/html"]