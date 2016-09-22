import boto3
import base64
import settings
import uuid
import json

formats = [

    {
        "destination": "videos/mp4/filename.mp4",
        "transcodePolicy": "Welcome Standard MP4"
    },
    {
        "destination": "videos/webm/filename.webm",
        "transcodePolicy": "System preset: Generic 320x240"
    }
]

message = {

  "_type": "event",
  "_created": "2016-05-18T23:27:04.4538202+00:00",
  "message": "event::call-bunny",

  "params": {
    "dlcsId": "7/3/ae32f1b2",
    "jobId": str(uuid.uuid4()),
    "source": "sample.mp4",
    "formats": base64.b64encode(json.dumps(formats))
  }
}

sqs = boto3.resource('sqs', settings.REGION)
queue = sqs.get_queue_by_name(QueueName=settings.INPUT_QUEUE)
response = queue.send_message(MessageBody=json.dumps(message))
print "Response status: %s" % (response['ResponseMetadata']['HTTPStatusCode'],)
