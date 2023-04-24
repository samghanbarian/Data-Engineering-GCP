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

