#Import Required Libraries
import asyncio
from datetime import datetime
import time
from aioambient import Websocket
from aioambient.errors import WebsocketError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
"""
Configuration
"""
url = ''
token = ''
org = ''
bucket = ''
MAC_ADDRESS = ""
API_KEY = ""
APP_KEY = ""
client = InfluxDBClient(url=url, token=token, org=org)

def create_data_point(data):
    data_temp = data
         
    try:
        dateutc = str(data_temp.get('dateutc'))
    except BaseException as err:
        dateutc = None
        
    try:
        tempf = float(data_temp.get('tempf'))
    except BaseException as err:
        tempf = None
        
    try:
        battout = float(data_temp.get('battout'))
    except BaseException as err:
        battout = None
        
    try:
        humidity = float(data_temp.get('humidity'))
    except BaseException as err:
        humidity = None
    
    try:
        feelsLike = float(data_temp.get('feelsLike'))
    except BaseException as err:
        feelsLike = None
     
    try:
        dewPoint = float(data_temp.get('dewPoint'))
    except BaseException as err:
        dewPoint = None
        
    try:
        winddir = float(data_temp.get('winddir'))
    except BaseException as err:
        winddir = None
        
    try:
        windspeedmph = float(data_temp.get('windspeedmph'))
    except BaseException as err:
        windspeedmph = None
    
    try:
        windgustmph = float(data_temp.get('windgustmph'))
    except BaseException as err:
        windgustmph = None
        
    try:
        maxdailygust = float(data_temp.get('maxdailygust'))
    except BaseException as err:
        maxdailygust = None
    
    try:
        hourlyrainin = float(data_temp.get('hourlyrainin'))
    except BaseException as err:
        hourlyrainin = None
    
    try:
        eventrainin = float(data_temp.get('eventrainin'))
    except BaseException as err:
        eventrainin = None
        
    try:
        dailyrainin = float(data_temp.get('dailyrainin'))
    except BaseException as err:
        dailyrainin = None
        
    try:
        weeklyrainin = float(data_temp.get('weeklyrainin'))
    except BaseException as err:
        weeklyrainin = None
        
    try:
        monthlyrainin = float(data_temp.get('monthlyrainin'))
    except BaseException as err:
        monthlyrainin = None
        
    try:
        totalrainin = float(data_temp.get('totalrainin'))
    except BaseException as err:
        totalrainin = None
        
    try:
        lastRain = float(data_temp.get('lastRain'))
    except BaseException as err:
        lastRain = None
        
    try:
        solarradiation = float(data_temp.get('solarradiation'))
    except BaseException as err:
        solarradiation = None
        
    try:
        uv = float(data_temp.get('uv'))
    except BaseException as err:
        uv = None
    
    try:
        tempinf = float(data_temp.get('tempinf'))
    except BaseException as err:
        tempinf = None
        
    try:
        humidityin = float(data_temp.get('humidityin'))
    except BaseException as err:
        humidityin = None
        
    try:
        feelsLikein = float(data_temp.get('feelsLikein'))
    except BaseException as err:
        feelsLikein = None   
        
    try:
        dewPointin = float(data_temp.get('dewPointin'))
    except BaseException as err:
        dewPointin = None  
        
    try:
        baromrelin = float(data_temp.get('baromrelin'))
    except BaseException as err:
        baromrelin = None  
    
    try:
        baromabsin = float(data_temp.get('baromabsin'))
    except BaseException as err:
        baromabsin = None  
    
    try:
        temp1f = float(data_temp.get('temp1f'))
    except BaseException as err:
        temp1f = None  
        
    try:
        humidity1 = float(data_temp.get('humidity1'))
    except BaseException as err:
        humidity1 = None  
    
    try:
        feelsLike1 = float(data_temp.get('feelsLike1'))
    except BaseException as err:
        feelsLike1 = None  
        
    try:
        dewPoint1 = float(data_temp.get('dewPoint1'))
    except BaseException as err:
        dewPoint1 = None 
     
    try:
        temp2f = float(data_temp.get('temp2f'))
    except BaseException as err:
        temp2f = None  
        
    try:
        humidity2= float(data_temp.get('humidity2'))
    except BaseException as err:
        humidity2 = None  
    
    try:
        feelsLike2 = float(data_temp.get('feelsLike2'))
    except BaseException as err:
        feelsLike2 = None  
        
    try:
        dewPoint2 = float(data_temp.get('dewPoint2'))
    except BaseException as err:
        dewPoint2 = None 
        
    try:
        temp3f = float(data_temp.get('temp3f'))
    except BaseException as err:
        temp3f = None  
        
    try:
        humidity3= float(data_temp.get('humidity3'))
    except BaseException as err:
        humidity3 = None  
    
    try:
        feelsLike3 = float(data_temp.get('feelsLike3'))
    except BaseException as err:
        feelsLike3 = None  
        
    try:
        dewPoint3 = float(data_temp.get('dewPoint3'))
    except BaseException as err:
        dewPoint3 = None 
              
    
    try:
        batt1 = float(data_temp.get('batt1'))
    except BaseException as err:
        batt1 = None  
        
    try:
        batt2= float(data_temp.get('batt2'))
    except BaseException as err:
        batt2 = None  
    
    try:
        batt3 = float(data_temp.get('batt3'))
    except BaseException as err:
        batt3 = None  
        
    try:
        batt_co2 = float(data_temp.get('batt_co2'))
    except BaseException as err:
        batt_co2 = None 
        
                
    data_final = [Point("data_from_sensors")
              .tag("MAC",MAC_ADDRESS)
              .field("DateInUTC", dateutc)
              .field("TemperatureOutdoor", tempf)
              .field("BatteryOutdoor", battout)
              .field("HumidityOutdoor", humidity) 
              .field("FeelsLikeOutdoor", feelsLike)
              .field("DewPointOutdoor", dewPoint)
              .field("WindDirection", winddir)
              .field("WindSpeedMPH", windspeedmph)
              .field("WindGustMPH", windgustmph)
              .field("MaxDailyGust", maxdailygust)
              .field("HourlyRainInches", hourlyrainin)
              .field("EventRainInches", eventrainin)
              .field("DailyRainInches", dailyrainin)
              .field("WeeklyRainInches", weeklyrainin)
              .field("MonthlyRainInches", monthlyrainin)
              .field("TotalRainInches", totalrainin)
              .field("LastRainDate", lastRain)
              .field("SolarRadiation", solarradiation)
              .field("UVIndex", uv)
              .field("TemperatureIndoor", tempinf)
              .field("HumidityIndoor", humidityin)
              .field("FeelsLikeIndoor", feelsLikein)
              .field("DewPointIndoor", dewPointin)                 
              .field("BarometerRelativeIndoor", baromrelin)
              .field("BarometerAbsoluteIndoor", baromabsin)
              .field("TemperatureEngineRoom", temp1f)
              .field("HumidityEngineRoom", humidity1)
              .field("FeelsLikeEngineRoom", feelsLike1)
              .field("DewPointEngineRoom", dewPoint1)
              .field("TemperatureGalley", temp2f)
              .field("HumidityGalley", humidity2)
              .field("FeelsLikeGalley", feelsLike2)
              .field("DewPointGalley", dewPoint2)
              .field("TemperatureCrewQuart", temp3f)
              .field("HumidityCrewQuart", humidity3)
              .field("FeelsLikeCrewQuart", feelsLike3)
              .field("DewPointCrewQuart", dewPoint3)
              .field("Battery1", batt1)
              .field("Battery2", batt2)
              .field("Battery3", batt3)
              .field("Battery_co2", batt_co2)  
               ]
    return data_final

def create_data_subscriptionrecord(data):
    data_info = data['devices'][0]['info']
    data_geo_point = data['devices'][0]['info']['coords']['coords']
    data_address = data['devices'][0]['info']['coords']
    data_sensor = [Point("subscription_data")
              .tag("MAC",MAC_ADDRESS)
              .field("ShipName",data_info.get('name',None))
              .field("Latitude", data_geo_point.get('lat',None))
              .field("Longitude", data_geo_point.get('lon',None))
              .field("Address", data_address.get('address',None))
              .field("Location", data_address.get('location',None))
              .field("Elevation", data_address.get('elevation',None))
                  ]   
    return data_sensor

def create_log_data(message, is_error):
    data_error = [Point("sensors_log_messages")
              .tag("LogFrom",MAC_ADDRESS)
              .field("message", message)
              .field("is_error",is_error)
                  ]                   
    return data_error


def print_data(data):
    #client = InfluxDBClient(url=url, token=token, org=org)
    data_final = create_data_point(data)
    write_api = client.write_api(write_option=ASYNCHRONOUS)
    write_api.write(bucket, org , data_final ,write_precision='s')  
    print("Data received")

def print_disconnect():
    #client = InfluxDBClient(url=url, token=token, org=org)
    #data_log = create_log_data("Disconnected from the weather station",0)
    #write_api = client.write_api(write_option=ASYNCHRONOUS)
    #write_api.write(bucket, org , data_log ,write_precision='s') 
    print("Connected to the weather station")


def print_connect():
    #client = InfluxDBClient(url=url, token=token, org=org)
    #data_log = create_log_data("Connected to the weather station",0)
    #write_api = client.write_api(write_option=ASYNCHRONOUS)
    #write_api.write(bucket, org , data_log ,write_precision='s')   
    print("Connected to the weather station")

def print_subscribed(data):
    #client = InfluxDBClient(url=url, token=token, org=org)
    data_subs = create_data_subscriptionrecord(data)
    write_api = client.write_api(write_option=ASYNCHRONOUS)
    write_api.write(bucket, org , data_subs ,write_precision='s')           
    print(f"Subscription data received: {data}")


async def main() -> None:
    try:
        websocket = Websocket(APP_KEY, API_KEY)
        websocket.on_connect(print_connect)
        websocket.on_data(print_data)
        websocket.on_disconnect(print_disconnect)
        websocket.on_subscribed(print_subscribed)
        await websocket.connect()
        while True:
            print("Waiting for 5 minutes")
            await asyncio.sleep(300)
    except BaseException as err:
        #error_msg = "There was a websocket error: " + err
        #client = InfluxDBClient(url=url, token=token, org=org)
        #data_log = create_log_data(error_msg,1)
        #write_api = client.write_api(write_option=SYNCHRONOUS)
        #write_api.write(bucket, org , data_log ,write_precision='s')   
        print("Error Occured: ", err)
        return

loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
