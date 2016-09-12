import boto3
import settings
import logging


def create_job(transcoder, job_id, pipeline_id,  source, outputs):

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
        UserMetadata={'jobId': str(job_id)}
    )
    logging.debug("result: %s", str(result))
    if result is not None:
        status_code = result['ResponseMetadata']['HTTPStatusCode']
        if 200 <= status_code < 300:
            return result['Job']['Id']
    return None


def get_preset_map(transcoder):

    preset_map = {}
    paginator = transcoder.get_paginator('list_presets')
    for page in paginator.paginate():
        for preset in page.get('Presets'):
            preset_map[preset['Name']] = preset['Id']
    return preset_map


def delete_key(s3, bucket, key):

    s3.meta.client.delete_object(Bucket=bucket, Key=key)


def get_queue_by_name(sqs, name):

    return sqs.get_queue_by_name(QueueName=name)


def get_messages_from_queue(queue):

    return queue.receive_messages(WaitTimeSeconds=settings.POLL_INTERVAL)


def get_pipeline_by_name(transcoder, name):

    paginator = transcoder.get_paginator('list_pipelines')
    for page in paginator.paginate():
        for pipeline in page['Pipelines']:
            if pipeline['Name'] == name:
                return pipeline['Id']
    return None


def get_sqs_resource():

    return boto3.resource('sqs', settings.REGION)


def get_transcoder_client():

    return boto3.client('elastictranscoder', settings.REGION)


def get_s3_resource():

    return boto3.resource('s3')


