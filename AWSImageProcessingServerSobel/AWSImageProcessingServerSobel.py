# -*- coding: utf-8 -*-

import boto3
import numpy
import skimage
import os

s3 = boto3.resource("s3")
sqs_client = boto3.client('sqs')
sqs_res = boto3.resource('sqs')

inboxQueue = sqs_res.get_queue_by_name(QueueName='inboxSobel')
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
    print(filenames)
    return filenames

def downloadImage(filename, s3_res):
    s3_res.Bucket('bucketimgproc').download_file(filename, filename)

def uploadImage(filename, s3_res):
    s3_res.Bucket('bucketimgproc').upload_file(filename, filename)

######################
########MAIN##########
######################

if not os.path.exists("original"):
    os.makedirs("original")

while True:

    paths = receiveMessage(inboxQueue)


    for path in paths:
        downloadImage(path, s3)
        numpyImage = skimage.io.imread(path, as_gray=True)
        filename = path.replace("original", "")

        #Sobel

        if not os.path.exists("processed/sobel"):
            os.makedirs("processed/sobel")
        sob = skimage.filters.sobel(numpyImage)
        skimage.io.imsave("processed/sobel/" + filename, sob)

        uploadImage("processed/sobel/" + filename, s3)
        sendMessage(outboxQueue, "processed/sobel/" + filename)

        print("Sobel Filtered image sent")

        os.remove("original/" + filename)
        os.remove("processed/sobel" + filename)



