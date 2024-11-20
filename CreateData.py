from faker import Faker
import csv
import json
import pandas as pd
import pandas.io.json as pd_json

def createCsvData():
    output = open("data.csv", "w")
    fake=Faker()
    header = ['name', 'age', 'street', 'city', 'state', 'postcode', 'lng', 'lat']
    mywriter = csv.writer(output)
    mywriter.writerow(header)
    for r in range(1000):
        mywriter.writerow([fake.name(), 
                        fake.random_int(18,80, 1), 
                        fake.street_address(), 
                        fake.city(), 
                        fake.state(), 
                        fake.zipcode(), 
                        fake.longitude(), 
                        fake.latitude()]) 
    output.close()

def createJsonData():
    output = open('data.JSON', 'w')
    fake = Faker()

    allData = {}
    allData['records'] = []

    for i in range(1000):
        data={"name":fake.name(),"age":fake.random_int(min=18, max=80, step=1),
            "street":fake.street_address(),
            "city":fake.city(),"state":fake.state(),
            "zip":fake.zipcode(),
            "lng":float(fake.longitude()),
            "lat":float(fake.latitude())}
        allData['records'].append(data)
    
    json.dump(allData, output)


def readJsonData():
    with open('data.JSON', 'r') as df:
        df = json.load(df)
        df = pd.json_normalize(df, record_path='records')
        print(df.head())

def readCsvData():
    df=pd.read_csv('data.CSV')
    for i,r in df.iterrows():
        print(r['name'])
    df.to_json('fromAirflow.JSON',orient='records')

#createJsonData()
readCsvData()
#readJsonData()

