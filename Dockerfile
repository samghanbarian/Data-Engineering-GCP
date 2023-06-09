FROM prefecthq/prefect:2.7.7-python3.9

COPY requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY /flow /opt/prefect/flows
RUN mkdir /opt/prefect/data