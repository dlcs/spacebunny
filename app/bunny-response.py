import base64
import json
import logzero
from logzero import logger
import logging
import os
import signal
import sys
import traceback
import aws
import settings
import time
import datetime
import pytz


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
                    except Exception:
                        e = traceback.format_exc()
                        logger.error(f"Error processing message: {e}")
                        aws.send_message(error_queue, message.body)
                    finally:
                        message.delete()
    except Exception as e:
        logger.error(f"Error getting messages: {e}")
        raise e


def process_message(message):
    logger.debug("process_message()")

    data = json.loads(json.loads(message.body)['Message'])
    et_job_id = data['jobId']
    job_id = data['userMetadata']['jobId']
    start_time = int(data['userMetadata']['startTime'])
    dlcs_id = data['userMetadata']['dlcsId']
    source = data['input']['key']

    outputs = data['outputs']
    result_outputs = []
    success_count = 0
    error_count = 0

    full_job_data = aws.get_job_data(transcoder, et_job_id)
    store_job_data(settings.JOB_DATA_BUCKET, dlcs_id, json.dumps(full_job_data))
    output_info = {o['Key']: o for o in full_job_data['Job']['Outputs']}

    for output in outputs:

        generated_key = output['key']
        output_job_data = output_info[generated_key]
        preset_id = output['presetId']
        preset_name = preset_id_map.get(preset_id)
        if preset_name in inverse_policy_map:
            transcode_policy = inverse_policy_map[preset_name]
        else:
            transcode_policy = preset_name

        new_key = ""
        status = "error"
        if output['status'] == "Complete":
            new_key = get_final_key(generated_key)
            aws.move_s3_object(s3, settings.OUTPUT_BUCKET, output['key'], new_key)
            status = "success"
            success_count += 1
        else:
            error_count += 1
        result_output = {
            "destination": new_key,
            "transcodePolicy": transcode_policy,
            "status":  status,
            "detail": output_job_data.get('StatusDetail'),
            "size": output_job_data.get('FileSize'),
            "duration": output_job_data.get('DurationMillis'),
            "width": output_job_data.get("Width"),
            "height": output_job_data.get("Height")
        }
        result_outputs.append(result_output)

    outputs_string = base64.b64encode(json.dumps(result_outputs))

    if success_count > 0:
        if error_count > 0:
            global_status = 'partial'
        else:
            global_status = 'success'
    else:
        global_status = 'none'

    result = {
        "_type": "event",
        "_created": str(datetime.datetime.now(pytz.timezone('UTC'))),
        "message": "event::bunny-output",
        "params": {
            "jobId": job_id,
            "etJobId": et_job_id,
            "dlcsId": dlcs_id,
            "status": global_status,
            "clockTime": int(round(time.time() * 1000)) - start_time,
            "source": source,
            "outputs":  outputs_string
        }
    }

    result_string = json.dumps(result)
    aws.send_message(response_queue, result_string)


def store_job_data(bucket, key, data):
    aws.put_s3_object(s3, bucket, key, data)


def get_final_key(key):
    parts = key.split('/')
    return '/'.join(parts[2:])


def get_inverse_policy_map():
    original_map = settings.TRANSCODE_MAPPINGS
    return dict(zip(original_map.values(), original_map.keys()))


def get_messages_from_queue():
    messages = aws.get_messages_from_queue(notification_queue)
    return messages


def get_notification_queue():
    return aws.get_queue_by_name(sqs, settings.NOTIFICATION_QUEUE)


def get_response_queue():
    return aws.get_queue_by_name(sqs, settings.RESPONSE_QUEUE)


def get_error_queue():
    return aws.get_queue_by_name(sqs, settings.ERROR_QUEUE)


if __name__ == "__main__":

    if settings.DEBUG:
        logzero.loglevel(logging.DEBUG)
    else:
        logzero.loglevel(logging.INFO)

    requested_to_quit = False

    sqs = aws.get_sqs_resource()
    transcoder = aws.get_transcoder_client()
    s3 = aws.get_s3_resource()

    notification_queue = get_notification_queue()
    response_queue = get_response_queue()
    error_queue = get_error_queue()

    preset_id_map = aws.get_preset_map(transcoder, inverse=True)
    inverse_policy_map = get_inverse_policy_map()

    main()
