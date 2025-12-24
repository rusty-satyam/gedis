package main

import (
	"math"
)

const (
	MIN_LATITUDE  = -85.05112878
	MAX_LATITUDE  = 85.05112878
	MIN_LONGITUDE = -180.0
	MAX_LONGITUDE = 180.0

	LATITUDE_RANGE  = MAX_LATITUDE - MIN_LATITUDE
	LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE
	EARTH_RADIUS    = 6372797.560856 // in meters
)

// Coordinates represents a geographical point.
type Coordinates struct {
	Latitude  float64
	Longitude float64
}

// compactInt64ToInt32 performs the inverse of bit-spreading.
// It takes a 64-bit integer where bits are spread out (e.g., 0b010101...)
// and compacts them back into a contiguous 32-bit integer (e.g., 0b111...).
// This is used during decoding to separate the interleaved latitude/longitude bits.
func compactInt64ToInt32(v uint64) uint32 {
	result := v & 0x5555555555555555
	result = (result | (result >> 1)) & 0x3333333333333333
	result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F
	result = (result | (result >> 4)) & 0x00FF00FF00FF00FF
	result = (result | (result >> 8)) & 0x0000FFFF0000FFFF
	result = (result | (result >> 16)) & 0x00000000FFFFFFFF
	return uint32(result)
}

// convertGridNumbersToCoordinates translates the integer grid coordinates back into
// floating-point Latitude and Longitude.
func convertGridNumbersToCoordinates(gridLatitudeNumber, gridLongitudeNumber uint32) Coordinates {
	const precision = 1 << 26

	gridLatitudeMin := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber)/float64(precision))
	gridLatitudeMax := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber+1)/float64(precision))
	gridLongitudeMin := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber)/float64(precision))
	gridLongitudeMax := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber+1)/float64(precision))

	latitude := (gridLatitudeMin + gridLatitudeMax) / 2
	longitude := (gridLongitudeMin + gridLongitudeMax) / 2

	return Coordinates{Latitude: latitude, Longitude: longitude}
}

// GeospatialDecode converts a 52-bit integer Geohash back into Latitude/Longitude coordinates.
func GeospatialDecode(geoCode uint64) Coordinates {
	y := geoCode >> 1
	x := geoCode

	gridLatitudeNumber := compactInt64ToInt32(x)
	gridLongitudeNumber := compactInt64ToInt32(y)

	return convertGridNumbersToCoordinates(gridLatitudeNumber, gridLongitudeNumber)
}

func degToRad(deg float64) float64 {
	return deg * math.Pi / 180
}

// GeoDistance calculates the distance between two points using the Haversine formula.
// Result is in meters.
func GeoDistance(c1, c2 Coordinates) float64 {
	lat1 := degToRad(c1.Latitude)
	lon1 := degToRad(c1.Longitude)
	lat2 := degToRad(c2.Latitude)
	lon2 := degToRad(c2.Longitude)

	dLat := lat2 - lat1
	dLon := lon2 - lon1

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*
			math.Sin(dLon/2)*math.Sin(dLon/2)

	c := 2 * math.Asin(math.Sqrt(a))

	return EARTH_RADIUS * c
}

// RadiusToMeters normalizes distance units (km, miles, ft) into meters
// for consistent internal calculation.
func RadiusToMeters(radius float64, unit string) float64 {
	switch unit {
	case "m":
		return radius
	case "km":
		return radius * 1000
	case "mi":
		return radius * 1609.344
	case "ft":
		return radius * 0.3048
	default:
		return radius // Default to meters
	}
}

// spreadInt32ToInt64 takes a 32-bit integer and "spreads" its bits apart.
// Input:  0b1111 (0...00001111)
// Output: 0b01010101 (inserts a 0 between every original bit)
// This is used to prepare the bits for interleaving.
func spreadInt32ToInt64(v uint32) uint64 {
	result := uint64(v)
	result = (result | (result << 16)) & 0x0000FFFF0000FFFF
	result = (result | (result << 8)) & 0x00FF00FF00FF00FF
	result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F
	result = (result | (result << 2)) & 0x3333333333333333
	result = (result | (result << 1)) & 0x5555555555555555
	return result
}

// interleave combines two 32-bit integers into one 64-bit integer by alternating bits.
// This creates the Geohash used for spatial indexing.
func interleave(x, y uint32) uint64 {
	xSpread := spreadInt32ToInt64(x)
	ySpread := spreadInt32ToInt64(y)

	yShifted := ySpread << 1

	return xSpread | yShifted
}

// GeospatialEncode converts Latitude/Longitude float coordinates into a 52-bit integer Geohash.
func GeospatialEncode(latitude, longitude float64) uint64 {
	const precision = 1 << 26

	normalizedLatitude := float64(precision) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE
	normalizedLongitude := float64(precision) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE

	latInt := uint32(normalizedLatitude)
	lonInt := uint32(normalizedLongitude)

	return interleave(latInt, lonInt)
}
