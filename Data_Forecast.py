import http.client
import json

conn = http.client.HTTPSConnection("visual-crossing-weather.p.rapidapi.com")

headers = {
    'X-RapidAPI-Host': "visual-crossing-weather.p.rapidapi.com",
    'X-RapidAPI-Key': "61c42c5c5amsh09f6aa4549815fep18194bjsna618281010e6"
}

conn.request("GET",
             "/forecast?aggregateHours=24&location=38.1156013%2C-121.619355&contentType=csv&unitGroup=us"
             "&shortColumnNames=0",
             headers=headers)

res = conn.getresponse()
data = res.read()
data_csv = data.decode("utf-8")
print(data.decode("utf-8"))

f = open('VCW_Forecast.csv', 'w')
f.write(data_csv)
f.close()

conn_tides = http.client.HTTPSConnection("tides.p.rapidapi.com")

headers_tides = {
    'X-RapidAPI-Host': "tides.p.rapidapi.com",
    'X-RapidAPI-Key': "61c42c5c5amsh09f6aa4549815fep18194bjsna618281010e6"
}

conn_tides.request("GET", "/tides?longitude=-121.619355&latitude=38.1156013&interval=60&duration=1440",
                   headers=headers_tides)

res_tides = conn_tides.getresponse()
data_tides = res_tides.read()

data_json = data_tides.decode("utf-8")
with open('tides_forecast.json', 'w') as outfile:
    outfile.write(json.dumps(data_json, indent=2))

conn_daily = http.client.HTTPSConnection("visual-crossing-weather.p.rapidapi.com")
conn_daily.request("GET",
             "/forecast?aggregateHours=1&location=38.1156013%2C-121.619355&contentType=csv&unitGroup=us&shortColumnNames=0",
             headers=headers)

res_daily = conn_daily.getresponse()
data_daily = res_daily.read()

print(data_daily.decode("utf-8"))
f = open('VCW_Forecast_Daily.csv', 'w')
f.write(data_daily.decode("utf-8"))
f.close()
