#!/usr/bin/env python3

import requests
import json
import pandas as pd
from database_init import engine
import time
import datetime
import hashlib

query_station = """
# In this example we fetch one station by it's ID
query station($stationId: ID!) {
  station(id: $stationId) {
    id
    external_id
    country_code
    name
    address
    city
    postal_code
    coordinates {
      latitude
      longitude
    }
    parking_type
    evses {
      uid
      evse_id
      status
      capabilities
      connectors {
        id
        standard
        format
        power_type
        power
        last_updated
      }
      parking_restrictions
      last_updated
      parking_cost
      properties
    }
    operator {
      id
      external_id
      name
    }
    opening_times {
      twentyfourseven
    }
    last_updated
    chargers {
      standard
      power
      price
      speed
    }
    speed
    status
    review {
      rating
      count
    }
  }
}
"""


query_connector = """
query station($stationId: ID!) {
  station(id: $stationId) {
    evses {
      connectors {
        id
        standard
        format
        power_type
        power
      }
    }
  }
}
"""

query_date = """
query station($stationId: ID!) {
  station(id: $stationId) {
    evses {
      last_updated
    }
  }
}
"""

query_operator = """
query station($stationId: ID!) {
  station(id: $stationId) {
    operator {
        id
        name
    }
  }
}
"""

query_evses = """
query station($stationId: ID!) {
  station(id: $stationId) {
    evses {
      evse_id
      status
    }
  }
}
"""



# global variables for current transaciton (we can have more transactions in one context eg. some tables have more than one value after a transaction)
station_id = {}
station_id["id"] = []
operator_id = {}
operator_id["id"] = []
connectors_id = {}
connectors_id["id"] = []
date_id = {}
date_id["id"] = []

dict_evses = {}
dict_evses["id"] = []
dict_evses["status"] = []


# from fetched station of 'station_around' api call (id is a stack, use pop)
progress_id = {}
progress_id["id"] = []

def main_req(query_station, query_connector, query_date, query_operator, query_evses, header, coords,  now, distance=5000):
    # request of api (form one request extract data and insert into database)
    global progress_id

    in_country = True

    distance = distance
    
    query_station_around = f"""
                            query stationAround {{
                            stationAround(
                                query: {{
                                location: {{ type: Point, coordinates: {coords} }}
                                distance: {distance}
                                power: [50, 22]
                                }}
                                size: 10
                                page: 0
                            ) {{
                                id
                                physical_address {{
                                country
                                city
                                street
                                number
                                postalCode
                                }}
                                power
                            }}
                            }}
                            """


    response_around = requests.post(url, json={'query': query_station_around}, headers=header)
    json_data_around = json.loads(response_around.text)
    around = json_data_around["data"]["stationAround"]


    try:
        for i in range(len(around)):
            if around[i]["physical_address"]["country"] != "DE":
                in_country = False
                break
            else:
                progress_id["id"].append(around[i]["id"]) # append id for later use

        #print(f"len of list around is {len(around)}")

        if in_country:
            for i in range(len(progress_id["id"])):
                new_id = progress_id["id"].pop()
                variables = {"stationId":"{}".format(new_id)}

                #print(variables, "<---------------------- var")


                response_station = requests.post(url, json={'variables': variables, 'query': query_station}, headers=header)
                response_connector = requests.post(url, json={'variables': variables, 'query': query_connector}, headers=header)
                response_date = requests.post(url, json={'variables': variables, 'query': query_date}, headers=header)
                response_operator = requests.post(url, json={'variables': variables, 'query': query_operator}, headers=header)
                response_evses = requests.post(url, json={'variables': variables, 'query': query_evses}, headers=header)

                json_data_station = json.loads(response_station.text)
                json_data_connector = json.loads(response_connector.text)
                json_data_date = json.loads(response_date.text)
                json_data_operator = json.loads(response_operator.text)
                json_data_evses = json.loads(response_evses.text)


                # ready to work data
                station = json_data_station["data"]["station"]
                connectors = json_data_connector["data"]["station"]["evses"]
                date = json_data_date["data"]["station"]["evses"]
                operator = json_data_operator["data"]["station"]["operator"]
                evses = json_data_evses["data"]["station"]["evses"]

                print("before database")
                "================insert to tables================"
                make_transaction(station, connectors, date, operator, evses, now)
    except:
        print("Error please check!")
    
    progress_id = {}
    progress_id["id"] = []


    pass


def req_user(coords:list , distance: int, now):

    query_station_around = f"""
                            query stationAround {{
                            stationAround(
                                query: {{
                                location: {{ type: Point, coordinates: {coords} }}
                                distance: {distance}
                                power: [50, 22]
                                }}
                                size: 10
                                page: 0
                            ) {{
                                id
                                physical_address {{
                                country
                                city
                                street
                                number
                                postalCode
                                }}
                                status
                            }}
                            }}
                            """

    response_around = requests.post(url, json={'query': query_station_around}, headers=header)
    json_data_around = json.loads(response_around.text)

    around = []
    try:
        around = json_data_around["data"]["stationAround"]
    except:
        print("Nochmal probieren.")
    
    appended_data = []
    for i in range(len(around)):
        new_dict = {}
        new_dict = around[i]["physical_address"]
        new_dict["status"] = around[i]["status"]
        frame = pd.DataFrame([new_dict])
        print("===========================================================")
        print(frame, "\n")
        print("===========================================================")

    main_req(query_station, query_connector, query_date, query_operator, query_evses, header, coords, now, distance)
    



# parsing (cleaning) data for station table
def clean_station_and_insert(station, engine=engine):
    global station_id
    for key in list(station.keys()):
        if key not in ["id", "country_code", "name", "address", "city", "postal_code", "speed", "coordinates", "status", "parking_type"]:
            del station[key]
        if key == "coordinates":
            station[key] = json.dumps(station[key])
    station_id["id"].append(station["id"])
    #print(station_id, "station_id <-----------------------")

    with engine.begin() as conn:
            try:
                pd.DataFrame([station]).to_sql("stations", conn, if_exists = "append", index=False)
            except:
                raise Exception("Already in: stations\n")


# parsing (cleaning) data
def clean_connectors_and_insert(connectors, engine=engine):
    global connectors_id
    for i in range(len(connectors)):
        for key in connectors[i].keys():
            connector = connectors[i][key][0]
            connector["format_connector"] = connector.pop("format")
            connectors_id["id"].append(connector["id"])

            with engine.begin() as conn:
                    try:
                        pd.DataFrame([connector]).to_sql("connectors", conn, if_exists = "append", index=False)
                    except:
                        raise Exception("Already in: connector\n")

    #print(connectors_id, "connectors_id <-----------------------")

# parsing (cleaning) datetime (last update of evses or connector)
def clean_date_and_insert(date, engine=engine):
    global date_id
    new_date = None
    for i in range(len(date)):
        new_date = datetime.datetime.strptime(date[i]["last_updated"], "%Y-%m-%dT%H:%M:%SZ")  
        new_date = new_date.strftime("%Y-%m-%d")
        m = new_date + str(int(time.time()*1000))
        m = hashlib.sha256(m.encode()).hexdigest()
        date_dict = {"id": m, "last_updated": "{}".format(new_date)}
        date_id["id"].append(date_dict["id"])

        with engine.begin() as conn:
            try:
                pd.DataFrame([date_dict]).to_sql("date", conn, if_exists = "append", index=False)
            except:
                continue
                #raise Exception("Already in: date\n")
    #print(date_id, "date <-----------------------")


# parsing (cleaning) datetime (last update of evses or connector)
def clean_operator_and_insert(operator, engine=engine):
    global operator_id

    operator_id["id"].append(operator["id"])
    with engine.begin() as conn:
            try:
                pd.DataFrame([operator]).to_sql("operator", conn, if_exists = "append", index=False)
            except:
                raise Exception("Already in: operator\n")
    
    #print(operator_id, "operator_id <-----------------------")
    

# fact table insertion in the last step (after all insertion of dimensions)
def clean_evses_and_insert(evses, now, engine=engine):
    # consider that you can only paste values which are also present in the dimensions
    global dict_evses, station_id, operator_id, connectors_id, date_id

    #print("************************************* HEEEEY **************************************************")

    for i in range(len(evses)):
        dict_evses["id"].append(evses[i]["evse_id"])
        dict_evses["status"].append(evses[i]["status"])

    # padding data (extend data; sparse rows get values)
    check_append_list(station_id, operator_id, connectors_id, date_id, dict_evses)
    local_dict = {"evses_id": dict_evses["id"], "station_id": station_id["id"], "operator_id": operator_id["id"], \
    "connectors_id":connectors_id["id"], "date_id": date_id["id"], "status_evses": dict_evses["status"], "aufruf": now}

    #print(local_dict)

    print("=================================............Write_Database..............====================================================")
    #time.sleep(10)
    with engine.begin() as conn:
        try:
            pd.DataFrame(local_dict).to_sql("evses", conn, if_exists = "append", index=False)
        except:
            raise Exception("Already in: evses\n")


# load into database
def make_transaction(station, connectors, date, operator, evses, now):
    global dict_evses, station_id, operator_id, connectors_id, date_id

    "================insert to tables================"
    try:
        clean_station_and_insert(station)
    except Exception as e:
        print(e)

    try:
        clean_connectors_and_insert(connectors)
    except Exception as e:
        print(e)

    try:
        clean_date_and_insert(date)
    except Exception as e:
        print(e)

    try:
        clean_operator_and_insert(operator)
        print("=================================............Wrote_to_Database..............====================================================")
        #time.sleep(4)
    except Exception as e:
        print(e)
    
    #print("************************************* IAM HERE **************************************************")

    #time.sleep(5)
    try:
        print("************************************* IAM HERE2 **************************************************")
        #time.sleep(5)
        clean_evses_and_insert(evses, now)
        print("************************************* IAM HERE3 **************************************************")
    except Exception as e:
        print(e)

    station_id["id"] = []
    operator_id["id"] = []
    connectors_id["id"] = []
    date_id["id"] = []
    dict_evses["id"] = []
    dict_evses["status"] = []

    #print(station_id, "<-------------->", dict_evses, "<------------>", operator_id, date_id, connectors_id)
    #print("*****************************************............Zustand herstellen................***************************************************************")
    #time.sleep(2)

def check_append_list(station_id, operator_id, connectors_id, date_id, evses):
    max_len = max([len(station_id["id"]), len(operator_id["id"]), len(connectors_id["id"]), len(date_id["id"]), len(dict_evses["status"]), len(dict_evses["id"])])
    #print("=============MAXLEN=========", max_len, "=======================")
    #time.sleep(7)
    for i in range(max_len):
        if len(station_id["id"]) < max_len:
            if len(station_id["id"]) == 0: 
                station_id["id"].append("Undefined")
            station_id["id"].append(station_id["id"][0])
        if len(operator_id["id"]) < max_len:
            if len(operator_id["id"]) == 0: 
                operator_id["id"].append("Undefined")
            operator_id["id"].append(operator_id["id"][0])
        if len(connectors_id["id"]) < max_len:
            if len(connectors_id["id"]) == 0: 
                connectors_id["id"].append("Undefined")
                continue
            connectors_id["id"].append(connectors_id["id"][0])
        if len(date_id["id"]) < max_len:
            if len(date_id["id"]) == 0: 
                date_id["id"].append("Undefined")
            date_id["id"].append(date_id["id"][0])
        if len(dict_evses["id"]) < max_len:
            if len(dict_evses["id"]) == 0: 
                dict_evses["id"].append("Undefined")
            dict_evses["id"].append(date_id["id"][0])
        if len(dict_evses["status"]) < max_len:
            if len(dict_evses["status"]) == 0: 
                dict_evses["status"].append("Undefined")
            dict_evses["status"].append(date_id["status"][0])

    #print("=============MAXLEN 2=========", max_len, "=======================")
    #time.sleep(7)

# decoder from city to coordinates, e.g. heidelberg -> "lat": 49.4093582, "lon": 8.694724
def geo_decoder(city: str) -> list:
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid=27ca854ef277b992d9a0325126877e3d"

    querystring = {
    "city": "{}".format(city),
    "limit": 1,
    "appid": "27ca854ef277b992d9a0325126877e3d"
    }

    response = requests.get(url, params=querystring)
    json_data = json.loads(response.text)[0]

    coords = []

    if json_data["country"] != "DE":
        raise Exception("Its not in Germany (DE)")
    else:
        lat, lon = json_data["lat"], json_data["lon"]
        coords.append(lon), coords.append(lat)
    
    return coords # left lon, right lat (because of dependent "around" api)


if __name__ == "__main__":

    url = "https://api.chargetrip.io/graphql"
    header = {
    "x-client-id": "5ed1175bad06853b3aa1e492"
    }

    # main_req(query_station, query_connector, query_date, query_operator, query_evses, header)

    """variables2 = [{'stationId': '61187819522bfe0ba4b67fda'},
    {'stationId': '5f106564ee27696ea95d1bc7'},
    {'stationId': '61187819522bfe0ba4b67fda'},
    {'stationId': '5ed11a88b899aea70c982614'},
    {'stationId': '5ed11ca3b899ae5a37983932'}]

    variables = {'stationId': '61187819522bfe0ba4b67fda'}

    print(len(variables2))

    for i in range(len(variables2)):
        response_station = requests.post(url, json={'variables': variables2[i], 'query': query_station}, headers=header)
        response_connector = requests.post(url, json={'variables': variables2[i], 'query': query_connector}, headers=header)
        response_date = requests.post(url, json={'variables': variables2[i], 'query': query_date}, headers=header)
        response_operator = requests.post(url, json={'variables': variables2[i], 'query': query_operator}, headers=header)
        response_evses = requests.post(url, json={'variables': variables2[i], 'query': query_evses}, headers=header)

        json_data_station = json.loads(response_station.text)
        json_data_connector = json.loads(response_connector.text)
        json_data_date = json.loads(response_date.text)
        json_data_operator = json.loads(response_operator.text)
        json_data_evses = json.loads(response_evses.text)

        # ready to work data
        station = json_data_station["data"]["station"]
        connectors = json_data_connector["data"]["station"]["evses"]
        date = json_data_date["data"]["station"]["evses"]
        operator = json_data_operator["data"]["station"]["operator"]
        evses = json_data_evses["data"]["station"]["evses"]

        "================insert to tables================"
        make_transaction(station, connectors, date, operator, evses)
        print(i)"""


    coords = None
    now = None


    while True:

        try:
            city = input("Suche EV-Stationen in Deutschland: ")
            distanz = input("Radius in Metern bitte [Wenn 5000m, dann 'y']: ")
            now = datetime.datetime.now()
            now = now.strftime("%d/%m/%Y, %H:%M:%S")
            if distanz == "y":
                distanz = 5000
            coords = geo_decoder(city)
            # eintritt zur datenbank und Anfrage
        except Exception as e:
            print(f"Bitte nochmal wählen und {e}")


        req_user(coords, distanz, now)

    """print(station_id)
    print(operator_id)
    print(connectors_id)
    print(date_id)
    print(len(connectors_id))"""
    
