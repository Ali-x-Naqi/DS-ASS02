import requests

lat, lon = 52.52, 13.41
start, end = '2025-01-01', '2025-12-31'

# Weather
w_url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={start}&end_date={end}&hourly=temperature_2m,relative_humidity_2m"
w_res = requests.get(w_url).json()

# Air Quality
aq_url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={lat}&longitude={lon}&start_date={start}&end_date={end}&hourly=pm10,pm2_5,nitrogen_dioxide,ozone"
aq_res = requests.get(aq_url).json()

print("Weather Hourly Keys:", w_res.get('hourly', {}).keys())
print("Weather len:", len(w_res.get('hourly', {}).get('temperature_2m', [])))
print("AQ Hourly Keys:", aq_res.get('hourly', {}).keys())
print("AQ len:", len(aq_res.get('hourly', {}).get('pm10', [])))
