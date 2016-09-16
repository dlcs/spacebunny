import base64
import json
import logging
import os
import sys
import aws
import settings
import time
import datetime
import pytz


class BunnyResponse(object):

    def __init__(self):
        self.sqs = None
        self.transcoder = None
        self.s3 = None
        self.notification_queue = None
        self.response_queue = None
        self.preset_map = None

    def run(self):

        self.set_logging()

        self.sqs = aws.get_sqs_resource()
        self.transcoder = aws.get_transcoder_client()
        self.s3 = aws.get_s3_resource()

        self.notification_queue = self.get_notification_queue()
        self.response_queue = self.get_response_queue()

        self.preset_map = aws.get_preset_map(self.transcoder, inverse=True)

        while True:
            try:
                while True:
                    if os.path.exists('/tmp/stop.txt'):
                        sys.exit()
                    for message in self.get_messages_from_queue():
                        if message is not None:
                            try:
                                self.process_message(message)
                            except:
                                logging.exception("Error processing message")
                                # TODO : send to error queue if unsuccessful ?
                            finally:
                                message.delete()
            except Exception:
                logging.exception("Error getting messages")
                # TODO : send to error queue?

    def process_message(self, message):

        data = json.loads(json.loads(message.body)['Message'])
        et_job_id = data['jobId']
        job_id = data['userMetadata']['jobId']
        start_time = job_id = int(data['userMetadata']['startTime'])
        source = data['input']['key']

        outputs = data['outputs']
        result_outputs = []
        success_count = 0
        error_count = 0

        for output in outputs:
            new_key = ""
            status = "error"
            if output['status'] == "Complete":
                new_key = self.get_final_key(output['key'])
                aws.move_S3_object(self.s3, settings.OUTPUT_BUCKET, output['key'], new_key)
                status = "success"
                success_count += 1
            else:
                error_count += 1
            result_output = {
                "destination": new_key,
                "transcodePolicy": self.preset_map.get(output['presetId']),
                "status":  status
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
                "elasticTranscodeJobId": et_job_id,
                "status": global_status,
                "clockTime": int(round(time.time() * 1000)) - start_time,
                "source": global_status,
                "outputs":  outputs_string
            }
        }

        result_string = json.dumps(result)
        aws.send_message(self.response_queue, result_string)

    @staticmethod
    def get_final_key(key):

        parts = key.split('/')
        return '/'.join(parts[2:])

    def get_messages_from_queue(self):

        messages = aws.get_messages_from_queue(self.notification_queue)
        return messages

    def get_notification_queue(self):

        return aws.get_queue_by_name(self.sqs, settings.NOTIFICATION_QUEUE)

    def get_response_queue(self):
        return aws.get_queue_by_name(self.sqs, settings.RESPONSE_QUEUE)

    @staticmethod
    def set_logging():

        logging.basicConfig(filename="bunny-response.log",
                            filemode='a',
                            level=logging.DEBUG,
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s', )
        logging.getLogger('boto').setLevel(logging.ERROR)
        logging.getLogger('botocore').setLevel(logging.ERROR)
        logging.getLogger('werkzeug').setLevel(logging.ERROR)


if __name__ == "__main__":

    bunny = BunnyResponse()
    bunny.run()