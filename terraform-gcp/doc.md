#convert ipynb to .py
- jupyter nbconvert --to=script load-data-postgres.ipynb load_data.py

#create ssh key to connect to vm machine
- cd .ssh
-ssh-keygen -t rsa -f gcp -C [username] -b 2048 #will name the file gcp

#connect to google compute instance
ssh -i ~/.ssh/gcp [username]@[external_ip]

#define a config file for connecting to instance in cloud
touch config #in ssh folder
code config

      Host [connection-name]
          HostName [machine external Ip]
          User [username ssh key]
          IdentificationFile [path to ssh key]


#install anaconda
  wget https://repo.anaconda.com/archive/Anaconda3-2023.03-1-Linux-x86_64.sh
  bash Anaconda3-2023.03-1-Linux-x86_64.sh

  #  install docker on ubuntu
  sudo apt-get install docker.io

  #configure tu run docker without root user(sudo)
    sudo groupadd docker  #add docker group
    sudo gpasswd -a $USER docker # add connected user $USER to group
    newgrp docker 

#install docker compose 
#download from githubrepo copy the download linkls
mkdir bin/
cd bin/
#https://github.com/docker/compose/releases #link for github dockercompose
wget https://github.com/docker/compose/releases/download/v2.17.3/docker-compose-linux-x86_64 -O docker-compose #out pou to docker-compose 
chmod +x docker-compose #tell system it is an executable file

#add docker compose to bashrc
nano .bashrc #open bashrc
export PATH="${HOME}/bin:${PATH}" #add the path to executable
source .bashrc #re-read bashrc

#see if the path is applied
which docker-compose #see the path to executable file
docker compose --version

#run docker-compose.yaml file after navigating to the dir where it exist
docker-compose up -d

#install pgcli
conda install -c conda-forge pgcli
pip install -U mycli 

#forward the port from vscode to our local machine
port 5432 #postgre
port 8080 #pgadmin
port 8888 #jupyter notebook


#install terraform
cd bin
wget https://releases.hashicorp.com/terraform/1.4.6/terraform_1.4.6_linux_386.zip
unzip terraform_1.4.6_linux_386.zip

#authenticate terraform with serviceaccount json file so we copy jsonfile to server
sftp gcp-ubu #at the dir where json file for service account is
mkdir .gck
cd .gck
put zeta-de-384407-5956edd4614c.json

#authenticate ti gcp
#set credential env for gcp
export GOOGLE_APPLICATION_CREDENTIALS="~/.gck/zeta-de-384407-5956edd4614c.json"
gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS #to authenticate to gcloud
gcloud auth activate-service-account zeta-user@zeta-de-384407.iam.gserviceaccount.com --key-file=/.gck/zeta-de-384407-5956edd4614c.json --project=zeta-de-384407
