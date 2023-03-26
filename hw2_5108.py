#!/usr/bin/env python
# coding: utf-8

# # CS653 2/2565
# Homework 2: Amazon S3 Select with Taxi Trip Data
# Name: นายชูเกียรติ เครือสวัสดิ์ Student ID: 6509035108
# สร้าง Amazon S3 bucket ชื่อ nyctlc-cs653-5108 โดยแทน xxxx ด้วยเลข 4 ตัวท้ายของรหัสน.ศ. ของตัวเอง 
# In[2]:


import boto3
import botocore
import pandas as pd
from IPython.display import display, Markdown

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')


# In[ ]:





# In[9]:


def create_bucket(bucket):
    import logging

    try:
        s3.create_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        logging.error(e)
        return 'Bucket ' + bucket + ' could not be created.'
    return 'Created or already exists ' + bucket + ' bucket.'


# In[25]:


create_bucket('nyctlc-cs653-5108')


# In[16]:


def list_buckets(match=''):
    response = s3.list_buckets()
    if match:
        print(f'Existing buckets containing "{match}" string:')
    else:
        print('All existing buckets:')
    for bucket in response['Buckets']:
        if match:
            if match in bucket["Name"]:
                print(f'  {bucket["Name"]}')


# In[26]:


list_buckets(match='nyctlc')


# In[20]:


def list_bucket_contents(bucket, match='', size_mb=0):
    bucket_resource = s3_resource.Bucket(bucket)
    total_size_gb = 0
    total_files = 0
    match_size_gb = 0
    match_files = 0
    for key in bucket_resource.objects.all():
        key_size_mb = key.size/1024/1024
        total_size_gb += key_size_mb
        total_files += 1
        list_check = False
        if not match:
            list_check = True
        elif match in key.key:
            list_check = True
        if list_check and not size_mb:
            match_files += 1
            match_size_gb += key_size_mb
            print(f'{key.key} ({key_size_mb:3.0f}MB)')
        elif list_check and key_size_mb <= size_mb:
            match_files += 1
            match_size_gb += key_size_mb
            print(f'{key.key} ({key_size_mb:3.0f}MB)')

    if match:
        print(f'Matched file size is {match_size_gb/1024:3.1f}GB with {match_files} files')            
    
    print(f'Bucket {bucket} total size is {total_size_gb/1024:3.1f}GB with {total_files} files')


# In[29]:


# pip install pyarrow


# In[27]:


list_bucket_contents(bucket='nyc-tlc', match='2017', size_mb=250)


# In[48]:


def preview(bucket,key):
    data_source = {
            'Bucket': bucket,
            'Key': key
        }
    # Generate the URL to get Key from Bucket
    url = s3.generate_presigned_url(
        ClientMethod = 'get_object',
        Params = data_source
    )

    data = pd.read_parquet(url, engine='pyarrow')
    return data
df=preview(bucket='nyc-tlc',key=f'trip data/yellow_tripdata_2017-01.parquet')
df.head(6)


# In[3]:


def key_exists(bucket, key):
    try:
        s3_resource.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The key does not exist.
            return(False)
        else:
            # Something else has gone wrong.
            raise
    else:
        # The key does exist.
        return(True)

def copy_among_buckets(from_bucket, from_key, to_bucket, to_key):
    if not key_exists(to_bucket, to_key):
        s3_resource.meta.client.copy({'Bucket': from_bucket, 'Key': from_key}, 
                                        to_bucket, to_key)        
        print(f'File {to_key} saved to S3 bucket {to_bucket}')
    else:
        print(f'File {to_key} already exists in S3 bucket {to_bucket}') 


# In[4]:


for i in range(1, 6):
    copy_among_buckets(from_bucket='nyc-tlc',
                       from_key=f'trip data/yellow_tripdata_2017-0{i}.parquet',
                       to_bucket='nyctlc-cs653-5108',
                       to_key=f'yellow_tripdata_2017-0{i}.parquet')


# a) ในเดือน Jan 2017 มีจ านวน yellow taxi rides ทั้งหมดเท่าไร แยกจ านวน rides ตาม
# ประเภทการจ่ายเงิน (payment)
# 

# In[37]:


import boto3

s3 = boto3.client('s3')
sum = 0

for i in range(1, 6):
    resp = s3.select_object_content(
        Bucket='nyctlc-cs653-5108',
        Key='yellow_tripdata_2017-01.parquet',
        ExpressionType='SQL',
        Expression=f"SELECT COUNT(payment_type) FROM s3object s WHERE payment_type={i}",
        InputSerialization={'Parquet': {}},
        OutputSerialization={'CSV': {}}
    )


# In[40]:


for event in resp['Payload']:
    if 'Record' in event:
        record= event['Records']['Payload'].decode('utf-8')
        sum=sum+int(records)
        print(f"มีyellow taxiทั้งหมด{sum}คัน")

# b) ในเดือน Jan 2017 yellow taxi rides ในแต่ละจุดรับผู้โดยสาร (Pickup location) เป็น
# จ านวน rides มากน้อยเท่าไร และมีค่าโดยสารรวมของ rides และจ านวนผู้โดยสารเฉลี่ยต่อ 
# rides ในแต่ละจุดเท่าไร       เนื่องจากคำสั่ง DISTINCT ไม่สามารถใช้กับ S3 Select จึงใช้คำสั่งของ pandas เพื่อ
# หาค่าทั้งหมดที่เป็นไปได้ของข้อมูล payment_type นั่นคือคำสั่ง 
# dataFrame.[‘payment_type’].unique() แล้วจัดเรียงค่าจากน้อยไปหามาก มีการคืนค่า
# มา 265 ค่าดังภาพ แปลว่ามีจุดรับผู้โดยสารรวม 265 แห่ง ดังภาพ
# In[50]:


import numpy as np
yellow_jan_PULocationID=df['PULocationID'].unique()
np.sort(yellow_jan_PULocationID)



# In[ ]:





# In[52]:


def cal_total_fare(id):
    resp = s3.select_object_content(
        Bucket='nyctlc-cs653-5108',
        Key='yellow_tripdata_2017-01.parquet',
        ExpressionType='SQL',
        Expression=f"SELECT SUM(total_amount) FROM s3object s WHERE PULocationID={id}",
        InputSerialization={'Parquet': {}},
        OutputSerialization={'CSV': {}}
    )
    for event in resp['Payload']:
        if 'Records' in event:
            record = event['Records']['Payload'].decode('utf-8')
            try:
                isinstance(float(record), float)
                return float(record)
            except:
                return None


# In[ ]:





# In[55]:


def cal_avg_passenger_count(id):
    resp = s3.select_object_content(
        Bucket='nyctlc-cs653-5108',
        Key='yellow_tripdata_2017-01.parquet',
        ExpressionType='SQL',
        Expression=f"SELECT AVG(passenger_count) FROM s3object s WHERE PULocationID={id}",
        InputSerialization={'Parquet': {}},
        OutputSerialization={'CSV': {}},
    )
    for event in resp['Payload']:
        if 'Records' in event:
            record = event['Records']['Payload'].decode('utf-8')
            try:
                isinstance(float(record), float)
                return float(record)
            except:
                return None

    
    


# In[56]:


pickUpLocationId=[]
total_fare_list=[]
avg_passenger_list=[]


# In[59]:


for event in resp['Payload']:
    if 'Records' in event:
        records = event['Records']['Payload'].decode('utf-8')
        pickUpLocationId.append(i)
        print(f"จุดรับผู้โดยสารจุดที่ {i} มีจำนวน rides ของ yellow taxi เท่ากับ {int(records)} ครั้ง")
        total_fare = float("{:.2f}".format(total_fare))
        total_fare_list.append(total_fare)
        print("ค่าโดยสารรวม", total_fare)
        avg_passenger = cal_avg_passenger_count(i)
        if isinstance(avg_passenger, float):
            avg_passenger = float("{:.2f}".format(avg_passenger))
            avg_passenger_list.append(avg_passenger)
            print(f"ค่าโดยสารรวม {avg_passenger} บาท")
        else:
            avg_passenger = 0.0
            avg_passenger_fare_list.append(total_fare)
            print("ค่าโดยสารรวม Not float")
        if isinstance(avg_passenger, float):
            avg_passenger = float("{:.2f}".format(avg_passenger))
            avg_passenger_list.append(avg_passenger)
            print(f"ค่าโดยสารรวม {avg_passenger} บาท")
        else:
            avg_passenger = 0.0
            avg_passenger_fare_list.append(total_fare)
            print("ค่าโดยสารรวม Not float")

        


# In[4]:


import pandas as pd

pickUpLocation = ['A', 'B', 'C']
total_fare_list = [50, 70, 100]
avg_passenger_list = [1.5, 2, 2.5]

data = {'จุดรับผู้โดยสารที่': pickUpLocation,
        'ค่าโดยสารรวม': total_fare_list,
        'จำนวนผู้โดยสารเฉลี่ยต่อรอบ': avg_passenger_list}

hw_item2 = pd.DataFrame(data)
hw_item2


# c) ในเดือน Jan - May 2017 มีจ านวน yellow taxi rides ทั้งหมดเท่าไร แยกจำนวน rides 
# ตามประเภทการจ่ายเงิน (payment)
# In[5]:


type1=[]
type2=[]
type3=[]
type4=[]
type5=[]
sum_ride=[]


# In[ ]:


import boto3

s3 = boto3.client('s3')

def cal_ride_each_month(month):
    sum = 0
    type1 = []
    type2 = []
    type3 = []
    type4 = []
    type5 = []
    sum_ride = []

    for type in range(1, 6):
        resp = s3.select_object_content(
            Bucket='nyctlc-cs653-5108',
            Key=f'yellow_tripdata_2017-0{month}.parquet',
            ExpressionType='SQL',
            Expression=f"SELECT COUNT(payment_type) FROM s3object s WHERE payment_type = {type}",
            InputSerialization={'Parquet': {}},
            OutputSerialization={'CSV': {}},
        )

        for event in resp['Payload']:
            if 'Records' in event:
                record = event['Records']['Payload'].decode('utf-8')
                records = int(record)
                sum = sum + records
                if type == 1:
                    type1.append(records)
                elif type == 2:
                    type2.append(records)
                elif type == 3:
                    type3.append(records)
                elif type == 4:
                    type4.append(records)
                else:
                    type5.append(records)

                print(f"จำนวน yellow taxi ride เดือน {month} ที่มี payment_type={type} เท่ากับ {records}")
        
        sum_ride.append(sum)
        print(f"rides เดือน {month} มี yellow taxi rides รวมทั้งสิ้น {sum} เที่ยว")
        print()

           
                    


# In[ ]:


import boto3

# define function cal_rides_each_month

for month in range(1, 6):
    cal_rides_each_month(month)


# In[ ]:





# In[ ]:


import pandas as pd

data={
    "month":['Jan','Feb','Mar','April','May'],
    "payment type1":type1,
    "payment type2":type2,
    "payment type3":type3,
    "payment type4":type4,
    "payment type5":type5,
    'sum':sum_rides
}

hw_item3=pd.DataFrame(data)
hw_item3

# การสะท้อนการเรียนรู้ของน.ศ.จากการบ้านครั้งนี้
# เราได้ความรู้และทักษะอะไรจากการทำการบ้านครั้งนี้บ้าง และคิดว่านำไปใช้ประโยชน์อย่างไรได้บ้าง
# -ผมคิดว่า ได้เรียนรู้การแก้ปัญหาโดยใช้ภาษาไพทอนและได้หัดใช้jupiter nootbookในAWS s3 เเละได้ทบทวนการใช้Github
# สิ่งที่เราชอบและไม่ชอบในการทำการบ้านครั้งนี้
# -ได้เรียนรู้สิ่งใหม่ๆ และเป็นการบังคับให้ผมพยายามเรียนรู้ และทำความเข้าใจเนื้อหาการเรียนทั้งหมดที่ผ่านมา แม้จะยังไม่เข้าใจทั้งหมดในตอนนี้ แต่ก็เป็นช่องทางให้เรียนรู้เพิ่มเติมได้ ในภายหลัง
# คิดว่าตัวเองควรปรับปรุงอย่างไร หรือ มีอะไรอย่างอื่นที่ควรได้รับการปรับปรุงสำหรับการบ้านครั้งต่อไป
# -เป็นปัญหาค่อนข้างยากมาก และให้เวลาทำน้อยเกินไปครับ ถ้าคนที่เข้าใจแต่ต้นก็น่าจะทำได้ครับ แต่ถ้าได้เวลานานกว่านี้ ก็คงดีครับ
