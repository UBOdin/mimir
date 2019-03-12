package mimir.util

import Math.{sin, toRadians, atan2, sqrt, cos, pow}

object GeoUtils {
  private val EARTH_AVG_RAD_KM = 6371
  def calculateDistanceInKilometer(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Int = {
    val difLat = toRadians( lat1 - lat2 )
    val difLon = toRadians( lon1 - lon2 )
    val sinLat = sin( difLat / 2 )
    val sinLng = sin( difLon / 2 )
    val asll = ( cos( toRadians( lat1 ) ) * cos( toRadians( lat2 ) ) * sinLng * sinLng ) + pow(sinLat, 2)
    val cas = atan2( sqrt( asll ), sqrt( 1 - asll )) * 2 
    (cas * EARTH_AVG_RAD_KM).toInt
  }
}