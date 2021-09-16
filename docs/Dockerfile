FROM python:3.7-slim
RUN apt-get update
RUN apt-get -y install git
RUN pip3 install -U sphinx==4.1.1 myst-parser==0.15.2 pygments==2.10.0 sphinx-rtd-theme==0.5.2 sphinx-autobuild==2021.3.14 gitpython==3.1.18
EXPOSE 8001
CMD ["sphinx-autobuild", "--host", "0.0.0.0", "--port", "8001", "/home/flink-cdc/docs", "/home/flink-cdc/docs/_build/html"]