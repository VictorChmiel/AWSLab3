# -*- coding: utf-8 -*-

import boto3
import numpy
import skimage
import os

s3 = boto3.resource("s3")
sqs_client = boto3.client('sqs')
sqs_res = boto3.resource('sqs')

inboxQueue = sqs_res.get_queue_by_name(QueueName='inboxHessian')
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

        #Hessian

        if not os.path.exists("processed/hessian"):
            os.makedirs("processed/hessian")
        sob = skimage.filters.hessian(numpyImage)
        # skimage.io.imshow(numpyImage)
        skimage.io.imsave("processed/hessian/" + filename, sob)

        uploadImage("processed/hessian/" + filename, s3)
        sendMessage(outboxQueue, "processed/hessian/" + filename)

        print("Hessian Filtered image sent")

        os.remove("original/" + filename)
        os.remove("processed/hessian" + filename)