# src/main.py
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict
from typing import Any # Important: ensure this is imported
import requests, os, json, time
import uuid
import db_cache
from COUNTRIES import COUNTRIES
from utils.google_places import get_google_places_suggestions_backend

app = FastAPI()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
@app.on_event("startup")
async def startup_event(): db_cache.init_db()

@app.get("/api/suggest-locations")
async def suggest_locations(query: str): return get_google_places_suggestions_backend(query, GOOGLE_PLACES_API_KEY, types='locality')

class WeatherRequest(BaseModel):
    type: str
    city: str = None
    zip: str = None; country: str = None
    lat: float = None; lon: float = None

# Define allowed editable columns per table
EDITABLE_TABLE_COLUMNS = {
    "weather_cache": ["data"],
    "user_queries": ["location_string"]
    # Add other tables here if they need editable fields
}

@app.post("/api/weather/current")
async def get_current_weather(req: WeatherRequest):
    lat, lon, location_string = None, None, None
    if req.type == 'city':
        geo_res = requests.get(f"http://api.openweathermap.org/geo/1.0/direct?q={req.city}&limit=1&appid={API_KEY}").json()
        if not geo_res:
            raise HTTPException(status_code=400, detail=f"City not found: {req.city}")
        lat, lon = geo_res[0]['lat'], geo_res[0]['lon']
        location_string = f"{geo_res[0].get('name')}, {geo_res[0].get('state', '')}, {geo_res[0].get('country')}".strip(', ')
    elif req.type == 'zip':
        country_code = next((v for k,v in COUNTRIES.items() if k == req.country), None)
        if not country_code: raise HTTPException(status_code=400, detail=f"Invalid country for zip: {req.country}")
        zip_res = requests.get(f"http://api.openweathermap.org/data/2.5/weather?zip={req.zip},{country_code}&appid={API_KEY}").json()
        if zip_res.get("cod") != 200: raise HTTPException(status_code=400, detail=f"Zip not found: {req.zip} ({zip_res.get('message')})")
        lat, lon = zip_res['coord']['lat'], zip_res['coord']['lon']
        location_string = f"{zip_res.get('name')}, {req.zip}"
    elif req.type == 'gps':
        if req.lat is None or req.lon is None: raise HTTPException(status_code=400, detail="Lat/Lon required for GPS.")
        lat, lon = req.lat, req.lon
        location_string = f"GPS_{lat}_{lon}"
    else: raise HTTPException(status_code=400, detail="Invalid request type.")

    session_id = "user_session_default"
    db_cache.log_user_query(session_id, location_string)

    cached_data = db_cache.get_cache(lat, lon, None, location_string)
    if cached_data:
        print("Fetching from cache...")
        if 'name' not in cached_data: cached_data['name'] = location_string
        if 'coord' not in cached_data: cached_data['coord'] = {'lat': lat, 'lon': lon}
        return cached_data
    else:
        print("Fetching from API...")
        fetched_data = requests.get(f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude=minutely,hourly,alerts&appid={API_KEY}&units=metric").json()
        if "current" not in fetched_data: raise HTTPException(status_code=500, detail=f"Failed to fetch weather: {fetched_data.get('message')}")
        fetched_data['name'] = location_string
        fetched_data['coord'] = {'lat': lat, 'lon': lon}
        data_ts = fetched_data['current'].get('dt', int(time.time()))
        db_cache.set_cache(lat, lon, location_string, fetched_data, data_ts)
        return fetched_data

# New Endpoints for Database Management

@app.get("/api/db/tables")
async def get_db_tables():
    return db_cache.get_table_names()

@app.get("/api/db/columns/{table_name}")
async def get_db_columns(table_name: str):
    return db_cache.get_table_columns(table_name)

@app.get("/api/db/pk_columns/{table_name}")
async def get_db_pk_columns(table_name: str):
    return db_cache.get_table_primary_key_columns(table_name)

# New endpoint to get editable columns for a table
@app.get("/api/db/editable_columns/{table_name}")
async def get_editable_columns(table_name: str):
    return EDITABLE_TABLE_COLUMNS.get(table_name, [])

@app.get("/api/db/data/{table_name}")
async def get_db_table_data(table_name: str, order_by_col: str = Query(None), order_direction: str = "ASC"):
    return db_cache.get_table_data(table_name, order_by_col, order_direction)

class RecordUpdate(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    pk_dict: dict
    field_to_update: str
    new_value: Any

@app.put("/api/db/record/{table_name}")
async def update_db_record(table_name: str, req: RecordUpdate):
    try:
        allowed_cols = EDITABLE_TABLE_COLUMNS.get(table_name)
        if allowed_cols is None:
            raise HTTPException(status_code=403, detail=f"Table '{table_name}' is not editable via this API.")
        if req.field_to_update not in allowed_cols:
            raise HTTPException(status_code=403, detail=f"Column '{req.field_to_update}' is not editable for table '{table_name}'. Only {', '.join(allowed_cols)} can be updated.")

        db_cache.update_record(table_name, req.pk_dict, req.field_to_update, req.new_value)
        return {"message": "Record updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating record: {e}")

class RecordDelete(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    pk_dict: dict

@app.delete("/api/db/record/{table_name}")
async def delete_db_record(table_name: str, req: RecordDelete):
    try:
        db_cache.delete_record(table_name, req.pk_dict)
        return {"message": "Record deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting record: {e}")
