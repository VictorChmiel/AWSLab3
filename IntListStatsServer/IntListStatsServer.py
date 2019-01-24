import boto3
import numpy
import datetime
import os
import time

def toIntList(l):
    return [int(value) for value in l if value != "" and value.isnumeric()]

def isCreatedQueue(queueName, sqs_client):
    '''une fonction qui permet de determiner si une queue a deja ete creee
    elle n'est pas parfaite, elle detecte si il existe une queue dont le nom contient queueName
    Permet d'enlever les caracteres non numeriques et les chaines vides si l'utilisateur separe
    les nombres par plusieurs espaces'''
    isCreated = False
    listQueues  = sqs_client.list_queues()
    listQueuesUrls = listQueues['QueueUrls']
    for value in listQueuesUrls:
        if queueName in value:
            isCreated = True
    return isCreated


######################
########MAIN##########
######################

#Chargement du service SQS
sqs_client = boto3.client('sqs')
sqs_res = boto3.resource('sqs')
s3 = boto3.resource('s3')

# Creation de la queue d'envoi du client si elle n'existe pas /Chargement de la queue d'envoi du client si elle existe
if isCreatedQueue("requestQueue", sqs_client):
    requestQueue: object = sqs_res.get_queue_by_name(QueueName='requestQueue')
else:
    requestQueue = sqs_res.create_queue(QueueName='requestQueue')

# Creation de la queue d'envoi du serveur si elle n'existe pas / Chargement de la queue d'envoi du serveur si elle existe
if isCreatedQueue("responseQueue", sqs_client):
    responseQueue: object = sqs_res.get_queue_by_name(QueueName='responseQueue')
else:
    responseQueue = sqs_res.create_queue(QueueName='responseQueue')

#Single Request*
while True:
    response = requestQueue.receive_messages()



    for message in response:
        body = message.body
        if(body.lower() == "exit"):
            message.delete()
            exit(0)
        toStat = toIntList(body.split(" "))
        print(toStat)

        mean = numpy.mean(toStat)
        std = numpy.std(toStat)
        max = numpy.max(toStat)
        min = numpy.min(toStat)
        median = numpy.median(toStat)

        toSend = "Entree : {0}\n"\
                 "Moyenne : {1}\n" \
                 "Ecart Type : {2}\n" \
                 "Max : {3}\n" \
                 "Min : {4}\n" \
                 "Mediane : {5}\n".format(body, mean, std, max, min, median)

        failure = True
        while (failure):
            response = responseQueue.send_message(MessageBody=toSend)
            # Failure ssi get Failure renvoie autre chose que None
            failure = response.get("Failure") is not None

        if not os.path.exists("log/ILS"):
            os.makedirs("log/ILS")
        now = str(datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f"))
        file = open( "log/ILS/" + now + ".log", "w")
        file.write(toSend)
        file.close()
        data = open("log/ILS/" + now + ".log", "rb")
        s3.Bucket('bucketils').put_object(Key=now + ".log", Body=data)
        data.close()
        os.remove("log/ILS/" + now + ".log")
        message.delete()



