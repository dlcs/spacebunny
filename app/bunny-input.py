import base64
import json
import logging
import logzero
from logzero import logger
import os
import signal
import sys
import aws
import settings
import random
import time

def lifecycle_continues():
    return not requested_to_quit


def signal_handler(signum, frame):
    logger.info("Caught signal %s" % signum)
    global requested_to_quit
    requested_to_quit = True


def setup_signal_handling():
    logger.info("setting up signal handling")
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


def main():
    logger.info("starting...")

    setup_signal_handling()

    try:
        while lifecycle_continues():
            for message in get_messages_from_queue():
                if message is not None:
                    try:
                        process_message(message)
                    except Exception as e:
                        logger.error("Error processing message: " + str(e))
                        aws.send_message(error_queue, message.body)
                    finally:
                        message.delete()
    except Exception as e:
        logger.error("Error getting messages: " + str(e))
        raise e


def process_message(message):
    logger.debug("process_message()")

    message_body = json.loads(message.body)

    if 'message' in message_body and message_body['message'] == "event::call-bunny":
        params = message_body.get('params')
        if params is None:
            logger.error('Empty params in message')
            return False
        job_id = params.get('jobId')
        source = params.get('source')
        dlcs_id = params.get('dlcsId')
        encoded_formats = params.get('formats')
        decoded_formats = base64.b64decode(encoded_formats)
        formats = json.loads(decoded_formats)
        outputs = map(lambda o: {'Key': o.get('destination'),
                                    'PresetId': get_preset_id(o.get('transcodePolicy'))}, formats)
        logger.debug("transcoding for jobId: %s" % (job_id,))
        transcodeJob = transcode_video(job_id, dlcs_id, source, outputs)
        metadataKey = "%s/metadata" % (dlcs_id)
        metadataBody = '<JobInProgress><ElasticTranscoderJob>%s</ElasticTranscoderJob></JobInProgress>' % transcodeJob['Job']['Id']
        aws.put_s3_object(s3, settings.METADATA_BUCKET, metadataKey, metadataBody)
    else:
        logger.info("Unknown message type received")
        return False, "Unknown message type"


def get_preset_id(policy_name):

    preset_name = policy_name
    if policy_name in settings.TRANSCODE_MAPPINGS:
        preset_name = settings.TRANSCODE_MAPPINGS.get(policy_name)
    v = preset_id_map.get(preset_name)
    logger.debug("get_preset_id (%s) = '%s'" % (policy_name, v))
    return v


def transcode_video(job_id, dlcs_id, source, outputs):

    for output in outputs:
        # prepend a folder, output will be copied back to original Key if job succeeds
        output['Key'] = get_random_prefix() + output['Key']
        aws.delete_s3_object(s3, settings.OUTPUT_BUCKET, output['Key'])

    metadata = {
        'jobId': str(job_id),
        'dlcsId': str(dlcs_id),
        'startTime': str(int(round(time.time() * 1000)))
    }
    return aws.create_job(transcoder, metadata, pipeline, source, outputs)


def get_messages_from_queue():
    messages = aws.get_messages_from_queue(input_queue)
    return messages


def get_input_queue():
    return aws.get_queue_by_name(sqs, settings.INPUT_QUEUE)


def get_error_queue():
    return aws.get_queue_by_name(sqs, settings.ERROR_QUEUE)


def get_pipeline():
    return aws.get_pipeline_by_name(transcoder, settings.PIPELINE)


def get_random_prefix():
    return "x/" + str(random.randint(0, 1000)).zfill(4) + "/"


if __name__ == "__main__":

    if settings.DEBUG:
        logzero.loglevel(logging.DEBUG)
    else:
        logzero.loglevel(logging.INFO)

    requested_to_quit = False

    sqs = aws.get_sqs_resource()
    transcoder = aws.get_transcoder_client()
    s3 = aws.get_s3_resource()
    input_queue = get_input_queue()
    error_queue = get_error_queue()
    pipeline = get_pipeline()
    preset_id_map = aws.get_preset_map(transcoder)

    main()
