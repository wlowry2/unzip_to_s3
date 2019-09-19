#!/usr/bin/env python3
import io
import re
import os
import boto3
import urllib
import mimetypes
from zipfile import ZipFile
from datetime import datetime
from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError
from botocore.exceptions import UnknownKeyError


def parse_content(zipped):
    # read in the streamingbody response as bytes and close
    sourceBodyStream = zipped['Body']
    sourceData = sourceBodyStream.read()
    sourceBodyStream.close()
    # read the streamingbody data and create a file-like object
    sourceBytes = io.BytesIO(sourceData)
    # open the zipped file in read mode
    zippedData = ZipFile(sourceBytes)
    # get all file bytes not directories from the file archive
    unzipped = []
    for name in zippedData.namelist():
        if not zippedData.getinfo(name).is_dir():
            fileName = zippedData.getinfo(name).filename
            body = zippedData.read(name)
            # tuple, mime is 1st position
            mime = mimetypes.guess_type(fileName)[0]
            contentType = 'binary/octet-stream' if not mime else mime
            objectParams = {
                'Name': fileName,
                'Body': body,
                'Type': contentType
            }
            unzipped.append(objectParams)
    # close archive
    zippedData.close()
    # return the unzipped bytes stream back to s3 for put object
    return unzipped


def handler(event, context):
    try:
        # aws cloud formation client
        s3_client = boto3.client('s3')

        # event is from the s3 bucket where the zip file was downloaded
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            # needs to escape '+' and encoded characters sign
            formattedKey = urllib.parse.unquote_plus(
                key,
                encoding='utf-8',
                errors='replace'
            )
            # zip converted to 'folder' so files will drop inside of it
            destination = formattedKey.split('.zip')[0] + '/'
            # use for copy object and S3 copy method
            sourceParams = {'Bucket': bucket, 'Key': formattedKey}
            bucketObjects = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=formattedKey,
                MaxKeys=1
            )
            if bucketObjects['KeyCount'] > 0:
                dropFileName = bucketObjects['Contents'][0]['Key']
                if dropFileName == formattedKey:
                    # create timestamp
                    timestamp = datetime.now().isoformat()
                    # create the new key
                    copyfilename = re.search('(?<=/)(.*)(?=.zip)', formattedKey)
                    copiedKey = os.environ['backupfoldername'] + '/'
                    copiedKey += copyfilename.group(0)
                    copiedKey += '_' + timestamp
                    copiedKey += '.zip'
                    # copy the file to the directory
                    s3_client.copy_object(
                        Bucket=bucket,
                        CopySource=sourceParams,
                        Key=copiedKey
                    )
            # unzip the file add upload the files into the new bucket
            zipped = s3_client.get_object(Bucket=bucket, Key=formattedKey)
            # unzip the byte stream and return to target bucket
            unzipped = parse_content(zipped)
            # upload the files with names and metadata
            for data in unzipped:
                s3_client.put_object(
                    Body=data['Body'],
                    Bucket=bucket,
                    Key=destination + data['Name'],
                    ContentEncoding='utf-8',
                    ContentType=data['Type'],
                    ACL='public-read'
                )

    except ClientError as err:
        print("Client error occurred: ", err)

    except ParamValidationError as err:
        print("ParamValidation error occurred: ", err)

    except UnknownKeyError as err:
        print("UnknownKey error occurred: ")
        print("Parameter: ", err.param)
        print("Value: ", err.value)
        print("Choices: ", err.choices)

    except Exception as err:
        print("Exception occurred: ", err)
