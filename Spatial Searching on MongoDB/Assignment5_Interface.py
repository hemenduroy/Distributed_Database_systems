#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
from math import radians, sin, cos, atan2, sqrt
#import re

def DistanceFunction(lat2, lon2, lat1, lon1):
    R = 3959 # miles
    var1 = radians(lat1)
    var2 = radians(lat2)
    varDelta = radians(lat2-lat1)
    varLambda = radians(lon2-lon1)
    varA = sin(varDelta/2) * sin(varDelta/2) + cos(var1) * cos(var2) * sin(varLambda/2) * sin(varLambda/2)
    varC = 2 * atan2(sqrt(varA), sqrt(1-varA))
    return R * varC

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    if cityToSearch is None or saveLocation1 is None or collection is None:
        return
        
    res = collection.find({"city": { "$regex" : "^" + cityToSearch + "$" , "$options" : "i"} },
                                 {"name":1, "full_address":1,"city":1, "state":1})
    if res is None:
        return

    output = []
    for i in res:
        name = i['name'].upper()
        full_address = i['full_address'].upper()
        city = i['city'].upper()
        state = i['state'].upper()

        line = name + "$" + full_address + "$" + city + "$" + state + "\n"
        output.append(line)
        
    f = open(saveLocation1, 'w')
    f.writelines(output)

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    if (categoriesToSearch is None or myLocation is None or maxDistance is None or saveLocation2 is None or collection is None):
        return

    tagged = collection.find({'categories' : { "$in" : categoriesToSearch }})
    if (tagged is None):
        return

    output = []
    i = (float(myLocation[0]), float(myLocation[1]))
    for location in tagged:
        name = location['name'].upper()
        latitude = location['latitude']
        longitude = location['longitude']

        d = DistanceFunction(i[0], i[1], latitude, longitude)
        if (d > maxDistance):
            continue

        output.append(name + "\n")

    if len(output) == 0:
        return
        
    f = open(saveLocation2, 'w')
    f.writelines(output)