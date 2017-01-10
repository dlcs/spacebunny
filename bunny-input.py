import base64
import json
import logging
import os
import sys
import aws
import settings
import random
import time
from boto3.s3.key import Key

class BunnyInput(object):

    def __init__(self):

        self.set_logging()
        self.sqs = aws.get_sqs_resource()
        self.transcoder = aws.get_transcoder_client()
        self.s3 = aws.get_s3_resource()
        self.input_queue = self.get_input_queue()
        self.error_queue = self.get_error_queue()
        self.pipeline = self.get_pipeline()
        self.preset_id_map = aws.get_preset_map(self.transcoder)

    def run(self):

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
                            aws.send_message(self.error_queue, message.body)
                        finally:
                            message.delete()
        except Exception as e:
            logging.exception("Error getting messages")
            raise e

    def process_message(self, message):

        message_body = json.loads(message.body)

        if 'message' in message_body and message_body['message'] == "event::call-bunny":
            params = message_body.get('params')
            if params is None:
                logging.error('Empty params in message')
                return False
            job_id = params.get('jobId')
            source = params.get('source')
            dlcs_id = params.get('dlcsId')
            encoded_formats = params.get('formats')
            decoded_formats = base64.b64decode(encoded_formats)
            formats = json.loads(decoded_formats)
            outputs = map(lambda o: {'Key': o.get('destination'),
                                     'PresetId': self.get_preset_id(o.get('transcodePolicy'))}, formats)
            logging.debug("transcoding for jobId: %s" % (job_id,))
            transcodeJob = self.transcode_video(job_id, dlcs_id, source, outputs)
            metadataKey = "%s/metadata" % (dlcs_id)
            metadataBody = '<JobInProgress><ElasticTranscoderJob>%s</ElasticTranscoderJob></JobInProgress>' % transcodeJob['Job']['Id']
            aws.put_s3_object(self.s3, settings.METADATA_BUCKET, metadataKey, metadataBody)
        else:
            logging.info("Unknown message type received")
            return False, "Unknown message type"

    def get_preset_id(self, policy_name):

        preset_name = policy_name
        if policy_name in settings.TRANSCODE_MAPPINGS:
            preset_name = settings.TRANSCODE_MAPPINGS.get(policy_name)
        v = self.preset_id_map.get(preset_name)
        logging.debug("get_preset_id (%s) = '%s'" % (policy_name, v))
        return v

    def transcode_video(self, job_id, dlcs_id, source, outputs):

        for output in outputs:
            # prepend a folder, output will be copied back to original Key if job succeeds
            output['Key'] = self.get_random_prefix() + output['Key']
            aws.delete_s3_object(self.s3, settings.OUTPUT_BUCKET, output['Key'])

        metadata = {
            'jobId': str(job_id),
            'dlcsId': str(dlcs_id),
            'startTime': str(int(round(time.time() * 1000)))
        }
        return aws.create_job(self.transcoder, metadata, self.pipeline, source, outputs)

    def get_messages_from_queue(self):

        messages = aws.get_messages_from_queue(self.input_queue)
        return messages

    def get_input_queue(self):

        return aws.get_queue_by_name(self.sqs, settings.INPUT_QUEUE)

    def get_error_queue(self):

        return aws.get_queue_by_name(self.sqs, settings.ERROR_QUEUE)

    def get_pipeline(self):

        return aws.get_pipeline_by_name(self.transcoder, settings.PIPELINE)

    @staticmethod
    def get_random_prefix():

        return "x/" + str(random.randint(0, 1000)).zfill(4) + "/"

    @staticmethod
    def set_logging():

        logging.basicConfig(filename="bunny-input.log",
                            filemode='a',
                            level=logging.DEBUG,
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s', )
        logging.getLogger('boto').setLevel(logging.ERROR)
        logging.getLogger('botocore').setLevel(logging.ERROR)
        logging.getLogger('werkzeug').setLevel(logging.ERROR)


if __name__ == "__main__":

    bunny = BunnyInput()
    bunny.run()
