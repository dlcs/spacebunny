import os

MESSAGES_PER_FETCH = 5
POLL_INTERVAL = 20

TRANSCODE_MAPPINGS = {
    'Welcome Standard MP4': 'System preset: Web',
    'Welcome Standard WebM': 'Wellcome WebM'
}

REGION = os.environ.get('BUNNY_AWS_REGION')  # e.g. 'eu-west-1'
INPUT_QUEUE = os.environ.get('BUNNY_INPUT_QUEUE')  # e.g. 'bunny-input'
ERROR_QUEUE = os.environ.get('BUNNY_ERROR_QUEUE')  # e.g. 'bunny-error'
NOTIFICATION_QUEUE = os.environ.get('BUNNY_NOTIFICATION_QUEUE')  # e.g. 'bunny-notification'
RESPONSE_QUEUE = os.environ.get('BUNNY_RESPONSE_QUEUE')  # e.g. 'bunny-response'
PIPELINE = os.environ.get('BUNNY_PIPELINE')  # e.g. bunny-pipeline'
OUTPUT_BUCKET = os.environ.get('BUNNY_OUTPUT_BUCKET')  # e.g. 'bunny-output'
JOB_DATA_BUCKET = os.environ.get('BUNNY_JOB_DATA_BUCKET')  # e.g. 'bunny-job-data'
