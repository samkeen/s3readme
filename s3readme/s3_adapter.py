import os

import boto3
import yaml


class S3Adapter:
    def __init__(self, aws_region, config_file_path, logger):
        """

        :param aws_region:
        :param config_file_path:
        :type logger: logger
        """
        self.logger = logger
        self.aws_region = aws_region
        self.config = self.read_config(config_file_path)
        self.aws_session = self.init_session()

    def read_config(self, config_file_path):
        """
        Read the yaml config file into a Dict

        :type config_file_path: str
        :return: Dict
        """
        if not os.path.isfile(config_file_path):
            raise RuntimeError('config does not exist at path: {}'.format(config_file_path))
        with open(config_file_path, 'r') as stream:
            try:
                return yaml.load(stream)
            except yaml.YAMLError as e:
                raise RuntimeError("Error while loading config[{}]: {}".format(config_file_path, e))

    def _aws_client(self, client_type, aws_region):
        return self.aws_session.client(client_type, region_name=aws_region)

    def init_session(self):
        """
        Create a session using local credentials or optionally assume and IAM role and create a session with that role
        see: http://boto3.readthedocs.io/en/latest/guide/session.html
        :return:
        """
        aws_session = None
        assume_role_arn = self.config.get('iam_role_to_assume', False)
        if assume_role_arn is not None:
            credentials = self.get_assumed_aws_role_credentials(assume_role_arn)
            self.logger.debug('Assuming the AWS IAM role: %s', assume_role_arn)
            aws_session = boto3.session.Session(
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
        else:
            # this will simply attempt to use your default profile in ~/.aws or ENV vars if they exist
            aws_session = boto3.session.Session()
        return aws_session

    def get_assumed_aws_role_credentials(self, role_arn):
        """
        Get API credentials for an assumed IAM Role

        :param role_arn:
        :return:
        """
        sts_client = boto3.client('sts')
        # Call the assume_role method of the STSConnection object and pass the role
        # ARN and a role session name.
        assumed_role_object = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName="AssumeSbarRole"
        )
        # From the response that contains the assumed role, get the temporary
        # credentials that can be used to make subsequent API calls
        aws_tmp_credentials = assumed_role_object['Credentials']

        return aws_tmp_credentials

    def get_paths(self, aws_region, bucket_name, prefix=None):
        s3_client = self._aws_client('s3', aws_region)
        s3_paths_list = []
        try:
            s3_folders = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix= prefix + '/' if prefix else '',
                Delimiter='/'
            )
            for prefix in s3_folders.get('CommonPrefixes', []):
                s3_paths_list.append(prefix.get('Prefix'))
        except Exception as e:
            self.logger.error('The was a problem listing objects for bucket: %s', bucket_name)
            self.logger.error(e)
            exit(1)
        return s3_paths_list
