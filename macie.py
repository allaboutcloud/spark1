import os 
import boto3
import re
import datetime as dt 
def lambda_handler(event, context): 
            client_roles= boto3.client('iam', region_name=os.environ['AWS_REGION'])
            client_macie=boto3.client('macie2',region_name=os.environ['AWS_REGION'])
            env_name=os.environ['ACCOUNT_NAME']
            aws_account_name= {"172670341454":"dev","745016539018":"sit","210080916680":"preprod","860215340995":"prod"}
            env=aws_account_name.get(env_name)
            bucket_name='vf-bdc-vb-euce1-'+str(env)+'-macie'
            tm = dt.datetime.now()
            jobtime=tm.strftime("%Y%m%d%H%M%S")
            jobname="macie-job-discovery-"+jobtime
            response = client_macie.create_classification_job(
                description='leap-macie-data-discovery',
                jobType='ONE_TIME',
                name=jobname,
                s3JobDefinition={
                    'bucketDefinitions': [
                        {
                            'accountId': env_name,
                            'buckets': [ bucket_name  ]
                        }
                        ],
                        'scoping': {
                            'includes': {
                                'and': [
                                    {
                                        'simpleScopeTerm': {
                                            'comparator': 'STARTS_WITH',
                                            'key': 'OBJECT_KEY',
                                            'values': [
                                                'source_system',
                                                ]
                                        }
                                    }
                                    ]
                            }
                        }
                }
                )
                
