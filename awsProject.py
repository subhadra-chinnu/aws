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
print("dns name",publicdns[0])
print("dns name",publicdns[1])
while(1):
    client=SSHClient()
    key_file = '/Users/subbu/Desktop/aws/1.11.180_1/bin/subbu_key.pem'
    host=publicdns[0]
    user='ec2-user'
    client.load_system_host_keys()
    client.load_host_keys(key_file)
    client.connect(hostname=host,username=user,key_filename=key_file)
    stdin, stdout, stderr = client.exec_command('top -bn1 | grep cpu')
    for line in iter(stdout.readline, ""):
        print("instance id to memory usage::",instanceids[0],line.split(" ").pop(29))
    client.close()

    client=SSHClient()
    key_file = '/Users/subbu/Desktop/aws/1.11.180_1/bin/subbu_key.pem'
    host=publicdns[1]
    user='ec2-user'
    client.load_system_host_keys()
    client.load_host_keys(key_file)
    client.connect(hostname=host,username=user,key_filename=key_file)
    stdin, stdout, stderr = client.exec_command('top -bn1 | grep cpu')
    for line in iter(stdout.readline, ""):
        print("instance id to memory usage::",instanceids[1],line.split(" ").pop(29))
    client.close()
    sleep(5)