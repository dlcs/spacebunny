import boto3
import botocore.exceptions
import json

import settings
from logzero import logger


def create_job(transcoder, metadata, pipeline_id,  source, outputs):
    logger.debug("create_job()")

    logger.debug("outputs: " + json.dumps(outputs))

    result = transcoder.create_job(
        PipelineId=pipeline_id,
        Input={
            'Key': source,
            'FrameRate': 'auto',
            'Resolution': 'auto',
            'AspectRatio': 'auto',
            'Interlaced': 'auto',
            'Container': 'auto'
        },
        Outputs=outputs,
        UserMetadata=metadata
    )
    logger.debug("result: %s", str(result))
    if result is not None:
        status_code = result['ResponseMetadata']['HTTPStatusCode']
        if 200 <= status_code < 300:
            jobId = result['Job']['Id']
            logger.debug(f"transcode Job ID: {jobId}")
            return jobId
    return None


def get_job_data(transcoder, job_id):
    logger.debug("get_job_data()")

    return transcoder.read_job(Id=job_id)


def get_preset_map(transcoder, inverse=False):
    logger.debug("get_preset_map()")

    preset_map = {}
    paginator = transcoder.get_paginator('list_presets')
    for page in paginator.paginate():
        for preset in page.get('Presets'):
            if inverse:
                preset_map[preset['Id']] = preset['Name']
            else:
                preset_map[preset['Name']] = preset['Id']
    return preset_map


def delete_s3_object(s3, bucket, key):
    logger.debug(f"delete_s3_object({bucket}, {key})")

    logger.info("Attempting to delete key %s from bucket %s" % (key, bucket))

    s3.meta.client.delete_object(Bucket=bucket, Key=key)


def move_s3_object(s3, bucket, old, new):
    logger.debug(f"move_s3_object({bucket}, {old}, {new})")

    logger.info("Attempting to move key %s to key %s in bucket %s" % (old, new, bucket))
    s3.meta.client.copy_object(CopySource={'Bucket': bucket, 'Key': old}, Bucket=bucket, Key=new)
    s3.meta.client.delete_object(Bucket=bucket, Key=old)


def put_s3_object(s3, bucket, key, data):
    logger.debug(f"put_s3_object({bucket}, {key})")

    s3.meta.client.put_object(Bucket=bucket, Key=key, Body=data)


def get_queue_by_name(sqs, name):
    logger.debug(f"get_queue_by_name({name})")

    return sqs.get_queue_by_name(QueueName=name)


def get_messages_from_queue(queue):
    logger.debug(f"get_messages_from_queue()")

    return queue.receive_messages(WaitTimeSeconds=settings.POLL_INTERVAL)


def get_pipeline_by_name(transcoder, name):
    logger.debug(f"get_pipeline_by_name({name})")

    paginator = transcoder.get_paginator('list_pipelines')
    for page in paginator.paginate():
        for pipeline in page['Pipelines']:
            if pipeline['Name'] == name:
                return pipeline['Id']
    return None


def send_message(queue, result_string):
    logger.debug(f"send_message({result_string})")

    queue.send_message(MessageBody=result_string)


def get_sqs_resource():
    return boto3.resource('sqs', settings.REGION)


def get_transcoder_client():
    return boto3.client('elastictranscoder', settings.REGION)


def get_s3_resource():
    return boto3.resource('s3', settings.REGION)
