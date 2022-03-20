import argparse
import boto3
from internet_scholar import read_dict_from_s3_url, AthenaLogger, AthenaDatabase, compress, read_dict_from_url, instantiate_ec2
import logging
import googleapiclient.discovery
from googleapiclient.errors import HttpError, UnknownApiNameOrVersion
from pathlib import Path
import json
from datetime import datetime
import time
import uuid
from socket import error as SocketError
import errno

class YoutubeChannelStatsConsumer:
    def __init__(self, athena_data, s3_admin, s3_data,
                 key_name, security_group, iam, init_script,
                 ami, instance_type, volume_size, region, name, s3_config):
        self.athena_data = athena_data
        self.s3_admin = s3_admin
        self.s3_data = s3_data
        self.key_name = key_name
        self.security_group = security_group
        self.iam = iam
        self.init_script = init_script
        self.ami = ami
        self.instance_type = instance_type
        self.volume_size = volume_size
        self.region = region
        self.name = name
        self.s3_config = s3_config

    LOGGING_INTERVAL = 100
    WAIT_WHEN_SERVICE_UNAVAILABLE = 30
    WAIT_WHEN_CONNECTION_RESET_BY_PEER = 60

    def collect_channel_stats(self):
        logging.info("Start collecting Youtube channel stats")

        sqs = boto3.resource('sqs')
        credentials_queue = sqs.get_queue_by_name(QueueName='youtube_credentials')
        message = credentials_queue.receive_messages()
        if len(message) == 0:
            raise Exception('No more credentials')
        else:
            credential = message[0].body
            message[0].delete()
            logging.info('Credential received!')

        try:
            youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                      version="v3",
                                                      developerKey=credential,
                                                      cache_discovery=False)
        except UnknownApiNameOrVersion as e:
            service = read_dict_from_url(url="https://www.googleapis.com/discovery/v1/apis/youtube/v3/rest")
            youtube = googleapiclient.discovery.build_from_document(service=service,
                                                                    developerKey=credential)

        channels_queue_empty = False
        quota_exceeded = False
        channels_queue = sqs.get_queue_by_name(QueueName='youtube_channels')

        output_json = Path(Path(__file__).parent, 'tmp', 'youtube_channel_stats.json')
        with open(output_json, 'w') as json_writer:
            while not channels_queue_empty and not quota_exceeded:
                message = channels_queue.receive_messages()
                if len(message) == 0:
                    channels_queue_empty = True
                else:
                    channels = json.loads(message[0].body)
                    message[0].delete()
                    logging.info('Channels received!')
                    channel_count = len(channels)
                    while len(channels) > 0 and not quota_exceeded:
                        channel = channels.pop()
                        num_channels = 0
                        if num_channels % self.LOGGING_INTERVAL == 0:
                            logging.info("%d out of %d channels processed", num_channels, channel_count)
                        num_channels = num_channels + 1

                        service_unavailable = 0
                        connection_reset_by_peer = 0
                        connection_timedout = 0
                        no_response = True
                        response = dict()
                        while no_response and not quota_exceeded:
                            try:
                                response = youtube.channels().list(part="statistics",id=channel).execute()
                                no_response = False
                            except SocketError as e:
                                if (e.errno != errno.ECONNRESET) and (e.errno != errno.ETIMEDOUT):
                                    logging.info("Other socket error!")
                                    raise
                                elif e.errno == errno.ETIMEDOUT:
                                    connection_timedout = connection_timedout + 1
                                    logging.info("Connection timed out! {}".format(connection_timedout))
                                    if connection_timedout <= 10:
                                        time.sleep(self.WAIT_WHEN_CONNECTION_RESET_BY_PEER)
                                        try:
                                            youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                                                      version="v3",
                                                                                      developerKey=credential,
                                                                                      cache_discovery=False)
                                        except UnknownApiNameOrVersion as e:
                                            service = read_dict_from_url(
                                                url="https://www.googleapis.com/discovery/v1/apis/youtube/v3/rest")
                                            youtube = googleapiclient.discovery.build_from_document(service=service,
                                                                                                    developerKey=credential)
                                    else:
                                        raise
                                else:
                                    connection_reset_by_peer = connection_reset_by_peer + 1
                                    logging.info("Connection reset by peer! {}".format(connection_reset_by_peer))
                                    if connection_reset_by_peer <= 10:
                                        time.sleep(self.WAIT_WHEN_CONNECTION_RESET_BY_PEER)
                                        try:
                                            youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                                                      version="v3",
                                                                                      developerKey=credential,
                                                                                      cache_discovery=False)
                                        except UnknownApiNameOrVersion as e:
                                            service = read_dict_from_url(
                                                url="https://www.googleapis.com/discovery/v1/apis/youtube/v3/rest")
                                            youtube = googleapiclient.discovery.build_from_document(service=service,
                                                                                                    developerKey=credential)
                                    else:
                                        raise
                            except HttpError as e:
                                if "403" in str(e):
                                    logging.info("Quota exceeded for developer key {}".format(credential))
                                    quota_exceeded = True
                                elif ("503" in str(e)) or ("500" in str(e)):
                                    if "503" in str(e):
                                        logging.info("Service unavailable")
                                    else:  # 500
                                        logging.info("Internal error encountered")
                                    service_unavailable = service_unavailable + 1
                                    if service_unavailable <= 10:
                                        time.sleep(self.WAIT_WHEN_SERVICE_UNAVAILABLE)
                                    else:
                                        raise
                                else:
                                    raise
                        if quota_exceeded:
                            channels.append(channel)
                            channels_queue.send_message(MessageBody=json.dumps(channels))
                            instantiate_ec2(key_name=self.key_name,
                                            security_group=self.security_group,
                                            iam=self.iam,
                                            init_script=self.init_script,
                                            ami=self.ami,
                                            instance_type=self.instance_type,
                                            size=self.volume_size,
                                            region=self.region,
                                            name=self.name,
                                            parameters=self.s3_config)
                        else:
                            for item in response.get('items', []):
                                item['retrieved_at'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                                json_writer.write("{}\n".format(json.dumps(item)))

        logging.info("Compress file %s", output_json)
        compressed_file = compress(filename=output_json, delete_original=True)

        s3 = boto3.resource('s3')
        s3_filename = "youtube_channel_stats/creation_date={}/{}-{}.json.bz2".format(
            datetime.utcnow().strftime("%Y-%m-%d"),
            uuid.uuid4().hex,
            num_channels)
        logging.info("Upload file %s to bucket %s at %s", compressed_file, self.s3_data, s3_filename)
        s3.Bucket(self.s3_data).upload_file(str(compressed_file), s3_filename)

        logging.info("Repair table for Youtube channel stats")
        athena = AthenaDatabase(database=self.athena_data, s3_output=self.s3_admin)
        athena.query_athena_and_wait(query_string="MSCK REPAIR TABLE youtube_channel_stats")

        logging.info("Concluded collecting channel stats - consumer")


def test_api_keys(s3_path):
    config = read_dict_from_s3_url(url=s3_path)
    credentials = config['youtube']
    current_key = 0
    for current_key in range(0, len(credentials)):
        try:
            youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                      version="v3",
                                                      developerKey=
                                                      credentials[current_key]['developer_key'],
                                                      cache_discovery=False)
        except UnknownApiNameOrVersion as e:
            service = read_dict_from_url(url="https://www.googleapis.com/discovery/v1/apis/youtube/v3/rest")
            youtube = googleapiclient.discovery.build_from_document(service=service,
                                                                    developerKey=credentials[current_key][
                                                                        'developer_key'])

        try:
            print('Email: {}'.format(credentials[current_key]['email']))
            print('Project: {}'.format(credentials[current_key]['project']))
            print('Key: {}'.format(credentials[current_key]['developer_key']))
            youtube.channels().list(part="statistics", id='UCYiM773ssvNMaBHvaWWeIoQ').execute()
            print('OK!')
        except Exception as e:
            print('Error! {}'.format(str(e)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='S3 Bucket with configuration', required=True)
    args = parser.parse_args()

    config = read_dict_from_s3_url(url=args.config)
    logger = AthenaLogger(app_name="youtube-channel-stats-consumer",
                          s3_bucket=config['aws']['s3-admin'],
                          athena_db=config['aws']['athena-admin'])
    try:
        youtube_channel_stats = YoutubeChannelStatsConsumer(athena_data=config['aws']['athena-data'],
                                                            s3_admin=config['aws']['s3-admin'],
                                                            s3_data=config['aws']['s3-data'],
                                                            key_name=config['aws']['key_name'],
                                                            security_group=config['aws']['security_group'],
                                                            iam=config['aws']['iam'],
                                                            init_script=config['aws']['init_script'],
                                                            ami=config['aws']['ami'],
                                                            instance_type=config['aws']['instance_type'],
                                                            volume_size=config['aws']['volume_size'],
                                                            region=config['aws']['default_region'],
                                                            name=config['aws']['name'],
                                                            s3_config=args.config)
        youtube_channel_stats.collect_channel_stats()
    finally:
        logger.save_to_s3()
        #logger.recreate_athena_table()


if __name__ == '__main__':
    main()
