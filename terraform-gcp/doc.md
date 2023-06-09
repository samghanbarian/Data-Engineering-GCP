#convert ipynb to .py
```bash
jupyter nbconvert --to=script load-data-postgres.ipynb load_data.py
```

#create ssh key to connect to vm machine
```bash
cd .ssh
ssh-keygen -t rsa -f gcp -C [username] -b 2048 #will name the file gcp
```

#connect to google compute instance
```bash
ssh -i ~/.ssh/gcp [username]@[external_ip]
```
#define a config file for connecting to instance in cloud #in ssh folder
```bash
touch config 
code config
```

      Host [connection-name]
          HostName [machine external Ip]
          User [username ssh key]
          IdentificationFile [path to ssh key]


#install anaconda
```bash
  wget https://repo.anaconda.com/archive/Anaconda3-2023.03-1-Linux-x86_64.sh
  bash Anaconda3-2023.03-1-Linux-x86_64.sh
```
  #  install docker on ubuntu

  ```bash
  sudo apt-get install docker.io
  ```

  #configure tu run docker without root user(sudo)
  ```bash
    sudo groupadd docker  #add docker group
    sudo gpasswd -a $USER docker # add connected user $USER to group
    newgrp docker 
```
#install docker compose 
#download from githubrepo copy the download links

```bash
mkdir bin/
cd bin/
#https://github.com/docker/compose/releases #link for github dockercompose
wget https://github.com/docker/compose/releases/download/v2.17.3/docker-compose-linux-x86_64 -O docker-compose #out pou to docker-compose 
chmod +x docker-compose #tell system it is an executable file
```
#add docker compose to bashrc
```bash
nano .bashrc #open bashrc
export PATH="${HOME}/bin:${PATH}" #add the path to executable
source .bashrc #re-read bashrc
```

#see if the path is applied
```bash
which docker-compose #see the path to executable file
docker compose --version
```

#run docker-compose.yaml file after navigating to the dir where it exist
```bash
docker-compose up -d
```

#install pgcli
```bash
conda install -c conda-forge pgcli
pip install -U mycli 
```

#forward the port from vscode to our local machine
port 5432 #postgre
port 8080 #pgadmin
port 8888 #jupyter notebook


#install terraform
```bash
cd bin
wget https://releases.hashicorp.com/terraform/1.4.6/terraform_1.4.6_linux_386.zip
unzip terraform_1.4.6_linux_386.zip
```

#authenticate terraform with serviceaccount json file so we copy jsonfile to server
```bash
sftp gcp-ubu #at the dir where json file for service account is
mkdir .gck
cd .gck
put zeta-de-384407-5956edd4614c.json
```
#authenticate ti gcp
#set credential env for gcp
```bash
export GOOGLE_APPLICATION_CREDENTIALS="home/sam/.gck/zeta-de-384407-5956edd4614c.json"
gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS #to authenticate to   gcloud
```

#create enve 
```bash
conda create -n prefect-test python=3.9
conda activate prefect-test # activate the env
```

install requirements in the env
```bash 
pip install -r requirement.txt
```
```bash
prefect orion start #start prefect UI  port 4200
```

list process running on different port
```bash 
lsof -i4 #ip4
kill [pid_process] #kill the process
```

Deployment in prefect - trigger and schedule the flow run - create a yaml file for the flow
```bash
prefect deployment build ./param_load_data_gcs.py:parent_flow -n "parametrised etl"
```
add the params to param section
```yaml
parameters: {'color':'yellow','year':2022,'month':[1,2,3]}
```
to run the deployment there should be an agent assign to it
```bash
perefct agent start --work-queue 'default'
```

define scheduling when defining deployment
```bash
prefect deployment build param_load_ygtaxi.py:parent_flow_yg -n green-yellow-taxi --cron "0 0 * * *" -a

wrapping etl in a container and running flows in container
in the Dockerfile
```bash
FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY /flow /opt/prefect/flows
RUN mkdir /opt/prefect/data
```

if there is an error with stat just make a new directory 'dockerfile" and copy the Dokerfile in and buil the image there