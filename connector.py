import requests

LAT, LON = 39.1031, -84.5120  # Cincinnati

def get_forecast():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def sync(config, state, schema):
    data = get_forecast()
    rows = []
    for t, temp, hum, prec, wind in zip(
        data["hourly"]["time"],
        data["hourly"]["temperature_2m"],
        data["hourly"]["relative_humidity_2m"],
        data["hourly"]["precipitation"],
        data["hourly"]["wind_speed_10m"]
    ):
        rows.append({
            "timestamp": t,
            "temperature_C": temp,
            "humidity_percent": hum,
            "precipitation_mm": prec,
            "wind_speed_mps": wind
        })
    yield "hourly_forecast", rows
