import json
import boto3
import datetime

s3 = boto3.resource('s3')

def validate_date_range(start_date,end_date):
    if start_date>end_date:
        raise RuntimeError
    return 

def date_within(date,start_date,end_date):
    return date<=end_date and date>=start_date

def validate_weekday_range(start_weekday,end_weekday):
    if start_weekday>end_weekday:
        raise RuntimeError
    return

def date_within_weekday(date,start_weekday,end_weekday):
    return start_weekday<=(date.weekday()+1) and end_weekday>=(date.weekday()+1)
    
def validate_period_range(start_period,end_period):
    if start_period>end_period:
        raise RuntimeError
  
def convert_period_and_date_to_datetime(period,date):
    dt=datetime.datetime(date.year,date.month,date.day)
    return 
(dt+datetime.timedelta(seconds=60*30)*(period-1)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+'Z'
    
def generate_tariff_from_template(tariff_template_json_like,date):
    # Tariff blocks are processed in order. Sequential 30 min blocks override previous 
defined
    # blocks. In this way the first block can be used to set a default price etc. If any of 
the
    # 48 tariff periods in the day are not defined at the end an exception will be raised. 
If 
    # any tariff period is ill defined an exception will be raised. 
    
    tariff=[None]*48
    
    for tariff_template_block_json in tariff_template_json_like["blocks"]:
        
        
start_date=datetime.datetime.strptime(tariff_template_block_json['start-date'],"%Y-%M-%d").date()
        
end_date=datetime.datetime.strptime(tariff_template_block_json['end-date'],"%Y-%M-%d").date()
        
        validate_date_range(start_date,end_date)
        
        #If date range validity is blank assume it applies to current date
        if date_within(date,start_date,end_date):
            
            start_weekday=int(tariff_template_block_json['start-weekday'])
            end_weekday=int(tariff_template_block_json['end-weekday'])
            
            validate_weekday_range(start_weekday,end_weekday)
            # If (both) weekday range values are blank assume it applies to current weekday.
            # Treat only one being set as ill-defined.
            
            if date_within_weekday(date,start_weekday,end_weekday):
                
                start_period=int(tariff_template_block_json['start-period'])
                end_period=int(tariff_template_block_json['end-period'])
                
                validate_period_range(start_period,end_period)
                
                Price=tariff_template_block_json['price']
                InjectionPrice=tariff_template_block_json['injection-price']
                Color=tariff_template_block_json['color']
                StatusDescription=tariff_template_block_json['status-description']
                
                
                for period in range(start_period,end_period+1):
                    
                    tariff[period-1]=(Price,InjectionPrice,Color,StatusDescription)
                    
    tariff_json_like=[]
    
    TariffID=tariff_template_json_like['tariffid']
    for index,tariff_period in enumerate(tariff):
        period=index+1
        if tariff_period is None:
            raise RuntimeError("Tariff period %i on %s is not defined."%(period,date))
        
        (Price,InjectionPrice,Color,StatusDescription)=tariff_period
                  
        tariff_period_json={}
        tariff_period_json['TariffID']=TariffID
        tariff_period_json['Price']=Price
        tariff_period_json['InjectionPrice']=InjectionPrice            
        tariff_period_json['Color']=Color
        tariff_period_json['StatusDescription']=StatusDescription
        tariff_period_json['StartTime']=convert_period_and_date_to_datetime(period,date)
        tariff_period_json['EndTime']=convert_period_and_date_to_datetime(period+1,date)
    
        tariff_json_like.append(tariff_period_json)
        
    return tariff_json_like

def get_tariff_template_data_from_s3(event, context):
    tariff_template_bucket_name=event['tariff-template-bucket-name']
    print("Retrieving tariff templates from S3")
    
    tariff_template_jsons_like=[]
    print("Getting tariffs from bucket '"+tariff_template_bucket_name+"'")
    bucket = s3.Bucket(tariff_template_bucket_name)
    object_summary_iterator = list(bucket.objects.all())
    
    for tariff_template_object in object_summary_iterator:
        
        
tariff_template_jsons_like.append(json.loads(tariff_template_object.get()['Body'].read()))

    return tariff_template_jsons_like

def delete_all_objects_from_s3_bucket(bucket_name):
    b = s3.Bucket(bucket_name)
    for x in b.objects.all(): 
        x.delete()

def save_string_to_s3_object(ostring,bucket_name,oname):
    o = s3.Object(bucket_name, oname)
    print(bucket_name,oname)
    o.put(Body=ostring.encode('utf-8'))


def lambda_handler(event, context):
    
    today=datetime.date.today()
    
    yesterday=today-datetime.timedelta(1)
    
    tomorrow=today+datetime.timedelta(1)
    
    tariff_bucket_name=event['tariff-bucket-name']
    
    # Get tariff templates from S3 
    tariff_template_jsons_like=get_tariff_template_data_from_s3(event,context)
    
    tariffs_jsons={}
    # Generate tariffs from templates
    for tariff_template in tariff_template_jsons_like:
        tariffs_json_like=[]
        for date in [yesterday,today,tomorrow]:
            tariffs_json_like.extend(generate_tariff_from_template(tariff_template,date))
        tariffs_jsons[tariff_template['tariffid']]=json.dumps(tariffs_json_like)
        
    # Delete existing tariffs from S3
    delete_all_objects_from_s3_bucket(tariff_bucket_name)
    
    # Save new tariffs to S3
    for tariffid,tariff_json in tariffs_jsons.items():
        save_string_to_s3_object(tariff_json,tariff_bucket_name,tariffid+'.json')
    
    return
