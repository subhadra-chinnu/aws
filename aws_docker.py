import boto3
from paramiko import SSHClient
from time import sleep

ec2 = boto3.resource('ec2')

instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
publicdns=[]
instanceids=[]

for instance in instances:
    publicdns.append(instance.public_dns_name)
    instanceids.append(instance.instance_id)
for a in publicdns:
    print(a)

while(1):
    client=SSHClient()
    key_file = '/Users/subbu/Desktop/aws/1.11.180_1/bin/subbu_key.pem'
    host=publicdns[0]
    user='ec2-user'
    client.load_system_host_keys()
    client.load_host_keys(key_file)
    client.connect(hostname=host,username=user,key_filename=key_file)

    client.exec_command("sudo yum update -y")
    client.exec_command("sudo yum install docker -y")
    client.exec_command("sudo service docker start")
    client.exec_command("sudo usermod -a -G docker ec2-user")
    client.close()

    client.connect(hostname=host,username=user,key_filename=key_file)

    client.exec_command("docker run -d -t ubuntu sh")
    stdin, stdout, stderr = client.exec_command("docker ps | grep ubuntu")
    containerId=""
    for line in iter(stdout.readline, ""):
        containerId=line.split(" ")[0]

    stdin, stdout, stderr = client.exec_command("docker exec " + containerId + " top -bn1 | grep CPU")
    for line in iter(stdout.readline, ""):
        print("instance to container to memory usage :: ",instanceids[0],containerId,line)
    client.close()

    client=SSHClient()
    key_file = '/Users/bh327611/homebrew/Cellar/awscli/1.11.180_1/bin/bharath_key.pem'
    host=publicdns[1]
    user='ec2-user'
    client.load_system_host_keys()
    client.load_host_keys(key_file)
    client.connect(hostname=host,username=user,key_filename=key_file)

    client.exec_command("sudo yum update -y")
    client.exec_command("sudo yum install docker -y")
    client.exec_command("sudo service docker start")
    client.exec_command("sudo usermod -a -G docker ec2-user")
    client.close()

    client.connect(hostname=host,username=user,key_filename=key_file)

    client.exec_command("docker run -d -t ubuntu sh")
    stdin, stdout, stderr = client.exec_command("docker ps | grep ubuntu")
    containerId=""
    for line in iter(stdout.readline, ""):
        containerId=line.split(" ")[0]

    stdin, stdout, stderr = client.exec_command("docker exec " + containerId + " top -bn1 | grep CPU")
    for line in iter(stdout.readline, ""):
        print("instance to container to memory usage :: ",instanceids[1],containerId,line)
    client.close()
    sleep(5)
