import base64
import json
import logging
import os
import sys
import aws
import settings


class BunnyInput(object):

    def __init__(self):
        self.sqs = None
        self.transcoder = None
        self.s3 = None
        self.input_queue = None
        self.pipeline = None
        self.preset_map = None

    def run(self):

        self.set_logging()

        self.sqs = aws.get_sqs_resource()
        self.transcoder = aws.get_transcoder_client()
        self.s3 = aws.get_s3_resource()

        self.input_queue = self.get_input_queue()
        self.pipeline = self.get_pipeline()

        self.preset_map = aws.get_preset_map(self.transcoder)

        while True:
            try:
                while True:
                    if os.path.exists('/tmp/stop.txt'):
                        sys.exit()
                    for message in self.get_messages_from_queue():
                        if message is not None:
                            self.process_message(message)
                            # TODO : send to error queue if unsuccessful
                            message.delete()
            except Exception:
                logging.exception("Error getting messages")
                # TODO : send to error queue

    def process_message(self, message):

        message_body = json.loads(message.body)

        if 'message' in message_body and message_body['message'] == "event::call-bunny":
            params = message_body.get('params')
            if params is None:
                logging.error('Empty params in message')
                return False
            job_id = params.get('jobId')
            source = params.get('source')
            encoded_formats = params.get('formats')
            decoded_formats = base64.b64decode(encoded_formats)
            formats = json.loads(decoded_formats)
            outputs = map(lambda o: {'Key': o.get('destination'),
                                     'PresetId': self.preset_map.get(o['transcodePolicy'])}, formats)
            logging.debug("transcoding for jobId: %s" % (job_id,))
            success = self.transcode_video(job_id, source, outputs)

        else:
            logging.info("Unknown message type received")
            return False, "Unknown message type"

    def transcode_video(self, job_id, source, outputs):

        for output in outputs:
            # prepend a folder, output will be copied back to original Key if job succeeds
            output['Key'] = 'x/' + output['Key']
            aws.delete_key(self.s3, settings.OUTPUT_BUCKET, output['Key'])

        return aws.create_job(self.transcoder, job_id,  self.pipeline, source, outputs)

    def get_messages_from_queue(self):

        messages = aws.get_messages_from_queue(self.input_queue)
        return messages

    def get_input_queue(self):

        return aws.get_queue_by_name(self.sqs, settings.INPUT_QUEUE)

    def get_pipeline(self):

        return aws.get_pipeline_by_name(self.transcoder, settings.PIPELINE)

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
