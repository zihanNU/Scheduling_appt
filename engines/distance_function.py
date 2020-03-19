"""
Prvoide different kinds of distance functions for usage
"""
import numpy as np


def approx_dist(**kwargs):
    """ distance approximation "haversine" function

        requires latitude and longitude of origin and destination cities (specified in decimal degrees)
        returns the "great circle" distance between the origin and destination in miles
    """
    # Convert decimal degrees to Radians:
    try:
        lat1 = np.radians(kwargs["origin_latitude"])
        lon1 = np.radians(kwargs["origin_longitude"])
        lat2 = np.radians(kwargs["dest_latitude"])
        lon2 = np.radians(kwargs["dest_longitude"])
    except KeyError as ex:
        raise Exception("Missing required argument: {}".format(repr(ex)))

    # Implementing Haversine Formula:
    dlon = (lon2 - lon1) / 2.0
    dlat = (lat2 - lat1) / 2.0
    a = np.sin(dlat) ** 2 + (np.cos(lat1) * np.cos(lat2) * (np.sin(dlon) ** 2))
    c = 2.0 * np.arcsin(np.sqrt(a))
    r = 3956.0  # radius constant in miles

    return c * r
