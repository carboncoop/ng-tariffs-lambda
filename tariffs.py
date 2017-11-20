import json
import boto3
import pika

s3 = boto3.resource('s3')

def get_tariff_data_from_external_data_source(event, context):
    pass

def get_tariff_data_from_s3(event, context):
    tariff_bucket_names=event['tariff-bucket-names']
    print("Retrieving tariffs from S3")
    for tariff_bucket_name in tariff_bucket_names:
        print("Getting tariffs from bucket '"+tariff_bucket_name+"'")
        bucket = s3.Bucket(tariff_bucket_name)
        
        tariff_jsons=[]
        object_summary_iterator = bucket.objects.all()

        for tariff_object in object_summary_iterator:
            print("Retrieved tariff file '"+tariff_object.key+"'")
            tariff_jsons.append(json.loads(tariff_object.get()['Body'].read()))

    return tariff_jsons

def create_or_update(tariff_json):
    pass

def create_connection(esb_host):
    credentials = pika.credentials.PlainCredentials('nobelgrid','nobelgrid')
    parameters = pika.ConnectionParameters(host=esb_host,port=5672,credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    return connection

def create_channel(connection):
    channel = connection.channel()
    channel.queue_declare(queue="tariffs_test", exclusive=False, auto_delete=False)
    channel.queue_bind(queue="tariffs_test",exchange='NobelgridExHeader',routing_key="Tariffs",arguments={"verb":"created","noun":"tariff_test","x-match":"all"})

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
    esb_host="10.0.0.85"
    print("Making connection to broker @ "+esb_host)
    connection=create_connection(esb_host)
    print("Connection established!")
    print("Forming channel...")
    channel=create_channel(connection)
    print("Channel formed!")

    tariff_jsons = get_tariff_data_from_s3(event, context)
    print(len(tariff_jsons)+" tariffs received")
    for tariff_json in tariff_jsons:
        print("Publishing tariff json ...")
        publish_tariff_json_to_esb(channel, tariff_json, "created")

if __name__ == "__main__":
    test_json = """[
    {

        "TariffID": "Coop_Balance ",

        "Price": 0.747,

        "InjectionPrice": 0.3,

        "Color": "red",

        "StatusDescription": "Highly Imbalance",

        "Imbalance": 0.8,

        "StartTime": "2015-01-01T00:00:00.000Z",

        "EndTime": "2015-01-01T00:15:00.000Z"

    },

    {

        "TariffID": "Coop_Balance",

        "Price": 0.143,

        "InjectionPrice": 0.35,

        "Color": "green",

        "StatusDescription": "Off-peak pricing",

        "StartTime": "2015-01-01T00:15:00.000Z",

        "EndTime": "2015-01-01T00:30:00.000Z"

    }

 ]"""
    connection=create_connection("10.0.0.85")
    channel=create_channel(connection)

    def on_q_declare(x):
        print(x)
    def on_q_bind(x):
        print(x)
    channel.queue_declare(queue="tariffs_test")
    channel.queue_bind(queue="tariffs_test",exchange='NobelgridExHeader',routing_key="Tariffs",arguments={"verb":"created","noun":"tariff_test","x-match":"all"})

    if channel.basic_publish(
        exchange='NobelgridExHeader',
        routing_key="Tariff",
        body=test_json,
        properties=pika.BasicProperties(
            headers={"verb":"created","noun":"tariff"},
            content_type='application/json',
            delivery_mode=1
        )
    ):
        print("Message published successfully")
    else:
        print("Message publish failed!")

    def consumer_callback(channel, method, properties,body):
        print(channel, method, properties,body)
        return

    channel.basic_consume(consumer_callback,queue="tariffs_test")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
            channel.stop_consuming()

    connection.close()
