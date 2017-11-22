import json
import datetime
import boto3
import pika
import requests 

s3 = boto3.resource('s3')

def get_tariff_data_from_be_market(event, context):
    pass

def get_tariff_data_from_uk_grid_carbon(event, context):
    print("Requesting carbon intensity from national grid API...")
    natgrid_api_status_to_ng_traffic_light_map={
        "very low":"green", 
        "low":"green", 
        "moderate":"yellow", 
        "high":"red",
        "very high":"red"
    }
    
    from_datetime=(datetime.datetime.now()-datetime.timedelta(seconds=60*30)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+'Z'
    natgrid_intensity_api_end_point=event["natgrid_intensity_api_end_point"]
    request_uri="intensity/%s/fw48h"%(from_datetime)
    
    headers = {
        'Accept': 'application/json'
    }
    r = requests.get(natgrid_intensity_api_end_point+'/'+request_uri, params={}, headers = headers)
    intensity_forecast_json_like=r.json()["data"]
    
    def convert_natgrid_intensity_api_datetime(natgrid_intensity_api_datetime_s):
        natgrid_intensity_api_datetime=datetime.datetime.strptime(natgrid_intensity_api_datetime_s,"%Y-%m-%dT%H:%MZ")
        return natgrid_intensity_api_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+'Z'
    
    tariff_json_like=[]
    for block in intensity_forecast_json_like:
        tariff_period_json_like={}
        tariff_period_json_like['TariffID']="GreenEnergyMax"
        tariff_period_json_like['Price']=float(block["intensity"]["forecast"])/1000.
        tariff_period_json_like['InjectionPrice']=0.05 #use rough value for solar PV intensity of 50 gCO2/kWh                
        tariff_period_json_like['Color']=natgrid_api_status_to_ng_traffic_light_map[block["intensity"]["index"]]
        tariff_period_json_like['StatusDescription']=block["intensity"]["index"]+" CO2 emissions."
        tariff_period_json_like['StartTime']=convert_natgrid_intensity_api_datetime(block["from"])
        tariff_period_json_like['EndTime']=convert_natgrid_intensity_api_datetime(block["to"])
    
        tariff_json_like.append(tariff_period_json_like)
    
    return tariff_json_like

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
        print('Published message to exchange')
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
    
    if event.get("natgrid_intensity_api_end_point",False):
        tariff_jsons.append([get_tariff_data_from_uk_grid_carbon(event, context)])
    
    # Publish tariffs to ESB
    for tariff_json in tariff_jsons:
        
        if not event.get("dummy-run",False):
            print("Publishing tariff json ...")
            publish_tariff_json_to_esb(channel, tariff_json, "created")
        else:
            print("Dummy run! JSON contents which would have been published are as follows:")
            print(tariff_json)

