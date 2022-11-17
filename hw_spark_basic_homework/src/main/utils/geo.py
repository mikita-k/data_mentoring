import pygeohash
from opencage.geocoder import OpenCageGeocode

from hw_spark_basic_homework.src.main.utils import properties


# pygeohash
def get_geohash(latitude, longitude):
    return pygeohash.encode(float(latitude), float(longitude), precision=4)


# OpenCageGeocode
def get_geocoder():
    key = properties.get_opencage_api_key()
    geocoder = OpenCageGeocode(key)
    return geocoder


def get_lat(address, city, country):
    geocoder = get_geocoder()
    return geocoder.geocode("{}, {}, {}".format(address, city, country))[0]['geometry']['lat']


def get_lng(address, city, country):
    geocoder = get_geocoder()
    return geocoder.geocode("{}, {}, {}".format(address, city, country))[0]['geometry']['lng']
