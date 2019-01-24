import boto3
import os

s3 = boto3.resource("s3")
sqs_client = boto3.client('sqs')
sqs_res = boto3.resource('sqs')

inboxQueueSobel = sqs_res.get_queue_by_name(QueueName='inboxSobel')
inboxQueueHessian = sqs_res.get_queue_by_name(QueueName='inboxHessian')

outboxQueue = sqs_res.get_queue_by_name(QueueName = 'outbox')

def sendMessage(Queue, filename):
    ##### Envoi d'un seul message #####

    failure = True
    while (failure):
        response = Queue.send_message(MessageBody=filename)
        # Failure ssi get Failure renvoie autre chose que None
        failure = response.get("Failure") is not None




def receiveMessage(Queue):
    received = False
    while (not received):

        # Ask for the response
        response = Queue.receive_messages()

        # print response if exists, exit loop
        filenames = []
        if len(response) > 0:
            for message in response:
                filenames.append(message.body)
                message.delete()
            received = True
    return filenames


if not os.path.exists("original"):
    os.makedirs("original")
    print("the file should be put in a folder named \'original\' at the root of the project\n")
    exit(-1)

filename = input("Enter the name of the file (the file should be put in a folder named \'original\' "
                 "at the root of the project\n")
path = "original/" + filename
data=open(path, 'rb')

s3.Bucket('bucketimgproc').put_object(Key= path, Body=data)

sendMessage(inboxQueueSobel, path)
sendMessage(inboxQueueHessian, path)


if not os.path.exists("processed/sobel"):
    os.makedirs("processed/sobel")

if not os.path.exists("processed/hessian"):
    os.makedirs("processed/hessian")

# The client expects two responses from the server

n_received = 0

while n_received <2 :
    for filename in receiveMessage(outboxQueue):
        s3.Bucket('bucketimgproc').download_file(filename, filename)
        n_received += 1

