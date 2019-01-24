# -*- coding: utf-8 -*-

import boto3
import time

def isCreatedQueue(queueName, sqs_client):
    '''une fonctionn qui permet de déterminer si une queue a déjà été créée
    elle n'est pas parfaite, elle détecte si il existe une queue dont le nom contient queueName
    Permet d'enlever les carachtères non numériques et les chaines vides si l'utilisateur sépare
    les nombres par plusieurs espaces'''

    isCreated = False
    listQueues  = sqs_client.list_queues()
    listQueuesUrls = listQueues['QueueUrls']
    for value in listQueuesUrls:
        if queueName in value:
            isCreated = True
    return isCreated


#Chargement du service SQS
sqs_client = boto3.client('sqs')
sqs_res = boto3.resource('sqs')



def sendMessage(requestQueue):
    ##### Envoi d'un seul message #####

    toSend = input("Entrez des entiers séparés par des espaces \n")
    failure = True
    while (failure):
        response = requestQueue.send_message(MessageBody=toSend)
        # Failure ssi get Failure renvoie autre chose que None
        failure = response.get("Failure") is not None



def receiveMessage(responseQueue):
    received = False
    while (not received):

        # Ask for the response
        response = responseQueue.receive_messages()

        # print response if exists, exit loop
        if len(response) > 0:
            for message in response:
                print(message.body)
                message.delete()
            received = True

######################
########MAIN##########
######################

# Creation de la queue d'envoi du client si elle n'existe pas /Chargement de la queue d'envoi du client si elle existe
if isCreatedQueue("requestQueue", sqs_client):
    requestQueue: object = sqs_res.get_queue_by_name(QueueName='requestQueue')
else:
    requestQueue = sqs_res.create_queue(QueueName='requestQueue')

# Creation de la queue d'envoi du serveur si elle n'existe pas /Chargement de la queue d'envoi du serveur si elle existe
if isCreatedQueue("responseQueue", sqs_client):
    responseQueue: object = sqs_res.get_queue_by_name(QueueName='responseQueue')
else:
    responseQueue = sqs_res.create_queue(QueueName='responseQueue')

continuer = True

while continuer:
    sendMessage(requestQueue)
    receiveMessage(responseQueue)
    charInput = input("Continuer ? (o/n)\n")
    while(charInput.lower() != "o" and charInput.lower()!= 'n' ):
        charInput = input("Mauvais Input \nContinuer ? (o/n)\n")
    if charInput == "n":
        requestQueue.send_message(MessageBody="exit")
        continuer = False
















