import json
import boto3
import pika

s3 = boto3.resource('s3')

def get_tariff_data_from_be_market(event, context):
    pass

def get_tariff_data_from_uk_grid_carbon(event, context):
    pass

def get_tariff_data_from_s3(event, context):
    tariff_bucket_names=event['tariff-bucket-names']
    print("Retrieving tariffs from S3")
    for tariff_bucket_name in tariff_bucket_names:
        print("Getting tariffs from bucket '"+tariff_bucket_name+"'")
        bucket = s3.Bucket(tariff_bucket_name)
        print("Init "+bucket.name+"'")
        tariff_jsons=[]
        object_summary_iterator = list(bucket.objects.all())
        
        for tariff_object in object_summary_iterator:
            print("Retrieved tariff file '"+tariff_object.key+"'")
            tariff_jsons.append(json.loads(tariff_object.get()['Body'].read()))

    return tariff_jsons

def create_connection(esb_host,esb_username,esb_password):
    credentials = pika.credentials.PlainCredentials(esb_username,esb_password)
    parameters = pika.ConnectionParameters(host=esb_host,port=5672,credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    return connection

def create_channel(connection):
    channel = connection.channel()
    #channel.queue_declare(queue="tariffs", exclusive=False, auto_delete=False)
    #channel.queue_bind(queue="tariffs",exchange='NobelgridExHeader',routing_key="Tariffs",arguments={"verb":"created","noun":"tariff","x-match":"all"})

    return channel

def publish_tariff_json_to_esb(channel, tariff_json, verb):
    if channel.basic_publish(
            exchange='NobelgridExHeader',
            routing_key='Tariff',
            body=json.dumps(tariff_json),
            properties=pika.BasicProperties(
                content_type='application/json',
                headers={"verb":verb, "noun":"tariff"},
            )
    ):
        print('Message publish was confirmed')
    else:
        print('Message could not be confirmed')

def lambda_handler(event, context):
    
    print("Making connection to broker @ "+event['esb-host'])
    connection=create_connection(event['esb-host'],event['esb-username'],event['esb-password'])
    channel=create_channel(connection)
    print("Connection established!")

    tariff_jsons=[]
    
    if event.get("tariff-bucket-names",False):
        tariff_jsons.append(get_tariff_data_from_s3(event, context))
        
    # if event.get("...") etc.
    # 
    
    # Publish tariffs to ESB
    for tariff_json in tariff_jsons:
        print("Publishing tariff json ...")
        publish_tariff_json_to_esb(channel, tariff_json, "created")

