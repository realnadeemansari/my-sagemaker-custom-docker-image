import json
import boto3
import datetime
import os


def lambda_handler(event, context):
    # TODO implement
    print("EVENT", event)
    try:
        sm_client = boto3.client("sagemaker")
        
        job_name = os.environ.get("JOB_NAME")
        training_image = os.environ.get("TRAINING_IMAGE")
        training_input_mode = os.environ.get("TRAINING_INPUT_MODE")
        role_arn = os.environ.get("ROLE_ARN")
        channel_name = os.environ.get("CHANNEL_NAME")
        s3_data_type = os.environ.get("S3_DATA_TYPE")
        s3_uri = os.environ.get("S3_URI")
        s3_data_distribution_type = os.environ.get("S3_DATA_DISTRIBUTION_TYPE")
        s3_output_path = os.environ.get("S3_OUTPUT_PATH")
        instance_type = os.environ.get("INSTANCE_TYPE")
        instance_count = int(os.environ.get("INSTANCE_COUNT"))
        volume_size_in_gb = int(os.environ.get("VOLUME_SIZE_IN_GB"))
        max_runtime_in_seconds = int(os.environ.get("MAX_RUNTIME_IN_SECONDS"))
        vpc_security_grp_ids = str(os.environ.get("SECURITY_GROUP_IDS")).split(",")
        vpc_subnets = str(os.environ.get("SUBNETS")).split(",")
        
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        folders = key.split("/")
        prefix = folders[0]
        result = {"message": "Not Started"}
        # print("s3://{}/{}/".format(bucket, folders[0]))
        if prefix == "data":
            data_location = "s3://{}/{}/".format(bucket, folders[0])
            print("Training started", datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
            result = sm_client.create_training_job(TrainingJobName=job_name,
                                 AlgorithmSpecification={
                                     "TrainingImage": training_image,
                                     "TrainingInputMode": training_input_mode
                                 },
                                 RoleArn=role_arn,
                                 InputDataConfig=[
                                     {
                                         "ChannelName": channel_name,
                                         "DataSource": {
                                             "S3DataSource": {
                                                 "S3DataType": s3_data_type,
                                                 "S3Uri": data_location,
                                                 'S3DataDistributionType': s3_data_distribution_type,
                                             }
                                         }
                                     }
                                 ],
                                 OutputDataConfig={
                                     'S3OutputPath': s3_output_path,
                                 },
                                 ResourceConfig={
                                    'InstanceType': instance_type,
                                    'InstanceCount': instance_count,
                                    'VolumeSizeInGB': volume_size_in_gb
                                },
                                VpcConfig={
                                    'SecurityGroupIds': vpc_security_grp_ids,
                                    'Subnets': vpc_subnets
                                },
                                StoppingCondition={
                                    'MaxRuntimeInSeconds': max_runtime_in_seconds
                                }
                            )
        print("RESULT", result)
    except Exception as e:
        print(str(e))
        result = {"ERROR": str(e)}
    return result
