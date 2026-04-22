# -*- coding: utf-8 -*-

from pyexpat import model
from wsgiref import headers
import json
import pandas as pd
import numpy as np
import math
from pathlib import Path
from datetime import datetime, timedelta, timezone
from pycaret.regression import load_model, predict_model
from fastapi import FastAPI, File, UploadFile, Form, Request
from fastapi.responses import JSONResponse, HTMLResponse
import uvicorn
import requests
import os
from sklearn.preprocessing import MinMaxScaler
import asyncpg
from app.database import init_pool, get_pool, close_pool
from fastapi import Depends
from contextlib import asynccontextmanager
from app.crud import insert_meteo, get_meteo, get_meteo_by_datetime, get_all_meteo, get_meteo_count, insert_prediction, update_meteo, delete_meteo, insert_prediction, get_prediction, get_model_names, get_prediction_by_datetime, get_all_predictions, get_prediction_count, delete_prediction
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from apscheduler.schedulers.background import BackgroundScheduler
import httpx


DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
HISTORY_DATA_API_URL = os.getenv("HISTORY_DATA_API_URL")
FORECAST_DATA_API_URL = os.getenv("FORECAST_DATA_API_URL")
KBELY = os.getenv("KBELY")
station_id, lat, lon, altitude = map(str, KBELY.split(","))
lat, lon, altitude = float(lat), float(lon), int(altitude)
FORECAST_HEADER = dict(item.split(": ") for item in os.getenv("FORECAST_HEADER").split(","))


app = FastAPI(
    title="Meteo Backend"
)



@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    await init_pool(
        host="meteo_postgres",
        port=5432,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )
    yield
    # shutdown
    await close_pool()



app = FastAPI(lifespan=lifespan)

templates = Jinja2Templates(directory="app/templates")
templates.env.filters["tojson"] = json.dumps
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Načtení trénovaných modelů
BASE_DIR = Path(__file__).resolve().parent
meteo_models_path = BASE_DIR / "models" / "meteopipeline"

def load_models(path: Path) -> dict[str, object]:
    models = {}
    for p in path.glob("*.pkl"):
        try:
            model_name = p.stem
            model = load_model(str(p))
            models[model_name] = model
            print(f"✅ Načten model: {model_name}")
        except Exception as e:
            print(f"❌ Chyba při načítání modelu {p.stem}: {e}")
    return models

meteo_models = load_models(meteo_models_path)





@app.get("/", response_class=HTMLResponse)
async def index(request: Request, 
                table: str = None, 
                model: str = None, 
                offset: int = 0, 
                limit: int = 100):
    try:    
        total_count = 0
        models = await get_model_names()
        if table == "meteo":
            data = await get_meteo(limit=limit, offset=offset)
            total_count = await get_meteo_count()
        elif table == "meteo_prediction":
            data = await get_prediction(limit=limit, offset=offset, model_name=model) if model != "None" else await get_prediction(limit=limit, offset=offset)
            total_count = await get_prediction_count(model_name=model) if model != "None" else await get_prediction_count()
        else:
            data = []
        return templates.TemplateResponse("index.html", {
            "request": request,
            "meteo_models": models,
            "selected_table": table,
            "selected_model": model,
            "data": data,
            "offset": offset,
            "limit": limit,
            "total_pages": math.ceil(total_count / limit) if total_count else 1,
            "total_count": total_count
        })
    except Exception as e:
        return HTMLResponse(
            content=f"Error: {e}, line: {e.__traceback__.tb_lineno}",
            status_code=500
        )   

@app.get("/meteo", response_class=HTMLResponse)
async def meteo_page(
    request: Request,
    limit: int = 100,
    offset: int = 0
):
    try:
        total_count = await get_meteo_count()
        result = await get_meteo(limit=limit, offset=offset)
        result = [
            {
                **dict(row),
                "Datetime": row["Datetime"].isoformat() if row["Datetime"] else None
            }
            for row in result
        ]
        return templates.TemplateResponse("meteo.html", {
            "request": request, 
            "data": result, 
            "offset": offset,
            "limit": limit,
            "total_pages": math.ceil(total_count / limit) if total_count else 1,
            "total_count": total_count
        })
    except Exception as e:
        return HTMLResponse(
            content=f"Error: {e}, line: {e.__traceback__.tb_lineno}",
            status_code=500
        )

@app.get("/api/meteo")
async def api_meteo(offset: int = 0, limit: int = 25):
    data = await get_meteo(limit=limit, offset=offset)
    total = await get_meteo_count()
    return {
        "data": [
            {
                **dict(row),
                "Datetime": row["Datetime"].isoformat() if row["Datetime"] else None
            }
            for row in data
        ],
        "total_count": total,
        "total_pages": math.ceil(total / limit),
        "offset": offset,
        "limit": limit,
    }

@app.post("/meteo/history")
async def fetch_historical_meteo(
    days: int = 5
):
    try:
        hours = 24 * days
        params = {
            "ids": station_id,
            "format": "json",
            "taf": "false",
            "hours": str(hours)
        }
        metar = requests.get(HISTORY_DATA_API_URL, params=params)
        metar.raise_for_status()

        data = metar.json()
        meteo_rows = []
        for row in data:
            meteo_row = {
                "Datetime" : datetime.fromisoformat(row["reportTime"].replace("Z", "+00:00")),
                "temp_out" : row["temp"],
                "dew_out" : row["dewp"],
                "winddir" : row["wdir"] if row["wdir"] != "VRB" else 0,
                "qnh" : row["altim"],
                "windspeed" : row["wspd"],
                "hum_out" : 0,  # Placeholder, will be calculated
                "is_forecast" : False
            }
            meteo_row["hum_out"] = 100 * (math.exp((17.625 * meteo_row["dew_out"]) / 
                                                   (243.04 + meteo_row["dew_out"])) 
                                          / math.exp((17.625 * meteo_row["temp_out"]) / 
                                                     (243.04 + meteo_row["temp_out"])))

            meteo_rows.append(meteo_row)
            
        
        await insert_meteo(meteo_rows)
        return {"message": "METAR saved to PostgreSQL", "rows": len(meteo_rows)}
    except Exception as e:
        return HTMLResponse(
            content=f"Error: {e}, line: {e.__traceback__.tb_lineno}",
            status_code=500
        )
    
@app.post("/meteo/forecast")
async def fetch_forecast_meteo():
    try:
        params = {
            "lat": lat,
            "lon": lon,
            "altitude": altitude
        }
        async with httpx.AsyncClient() as client:
            response = await client.get(FORECAST_DATA_API_URL, headers=FORECAST_HEADER, params=params)
            response.raise_for_status()
            data = response.json()
            
        meteo_rows = []
        for timepoint in data["properties"]["timeseries"]:
            meteo_row = {
                "Datetime": datetime.fromisoformat(timepoint["time"].replace("Z", "+00:00")),
                "temp_out": timepoint["data"]["instant"]["details"]["air_temperature"],
                "dew_out": timepoint["data"]["instant"]["details"]["dew_point_temperature"],
                "winddir": timepoint["data"]["instant"]["details"]["wind_from_direction"],
                "qnh": timepoint["data"]["instant"]["details"]["air_pressure_at_sea_level"],
                "windspeed": timepoint["data"]["instant"]["details"]["wind_speed"],
                "hum_out": timepoint["data"]["instant"]["details"]["relative_humidity"],
                "is_forecast": True
            }

            meteo_rows.append(meteo_row)

        await insert_meteo(meteo_rows)
        return {"message": "Forecast saved to PostgreSQL", "rows": len(meteo_rows)}
    except Exception as e:
        return HTMLResponse(
            content=f"Error: {e}, line: {e.__traceback__.tb_lineno}",
            status_code=500
        )

@app.get("/meteo_prediction")
async def prediction_page(
    request: Request,
    limit: int = 100,
    offset: int = 0
):
    try:
        models = await get_model_names()
        total_count = await get_prediction_count(model_names=models)
        result = await get_prediction(limit=limit, offset=offset)
        for model in result:
            result[model] = [
                {
                    **dict(row),
                    "Datetime": row["Datetime"].isoformat() if row["Datetime"] 
                    else None
                } for row in result[model]
            ]
        datetimes = []
        for records in result.values():
            for row in records:
                if row["Datetime"] not in datetimes:
                    datetimes.append(row["Datetime"])
        result["datetimes"] = datetimes

        return templates.TemplateResponse("pred.html", {
            "request": request, 
            "data": result, 
            "offset": offset,
            "limit": limit,
            "total_pages": {model: math.ceil(total_count[model] / limit) 
                            if total_count else 1 for model in models},
            "total_count": total_count,
            "meteo_models": models
        })
    except Exception as e:
        return HTMLResponse(
            content=f"Error: {e}, line: {e.__traceback__.tb_lineno}",
            status_code=500
        )

@app.get("/api/meteo_prediction")
async def api_meteo_prediction(offset: int = 0, limit: int = 25, model_names: list[str] = None):
    data = await get_prediction(limit=limit, offset=offset, model_names=model_names)
    for model, records in data.items():
        data[model] = [
            {
                **dict(row),
                "Datetime": row["Datetime"].isoformat() if row["Datetime"] else None
            }
            for row in records
        ]
        
    datetimes = []
    for records in data.values():
        for row in records:
            if row["Datetime"] not in datetimes:
                datetimes.append(row["Datetime"])
    data["datetimes"] = datetimes
    total = await get_prediction_count(model_names=model_names)
    return {
        "data": data,
        "total_count": total,
        "total_pages": {model: math.ceil(count / limit) 
                        for model, count in total.items()} 
                        if isinstance(total, dict) else math.ceil(total / limit),
        "offset": offset,
        "limit": limit,
    }


async def predict_and_insert(Datetime: list[datetime], overwrite: bool = False):
    try:
        result = await get_meteo_by_datetime(Datetime=Datetime)
        meteo_models = load_models(meteo_models_path)
        if not result:
            return None

        meteo_df = pd.DataFrame([{
            "temp_out": row["temp_out"],
            "dew_out": row["dew_out"],
            "winddir": row["winddir"],
            "qnh": row["qnh"],
            "windspeed": row["windspeed"],
            "hum_out": row["hum_out"]
        } for row in result])
        forecast_status = [row["is_forecast"] for row in result]
        scaler = MinMaxScaler()
        meteo_scaled = pd.DataFrame(
            scaler.fit_transform(meteo_df),
            columns=meteo_df.columns
        )
        print(meteo_models.items())
        last_predictions = await get_prediction_by_datetime(
            Datetime=Datetime[0],
            model_names=list(meteo_models.keys()),
            offset=1
            )
        for model_name, model in meteo_models.items():
            last_prediction = last_predictions.get(model_name)
            recent_sum = last_prediction.get("corr_sum", 0) if last_prediction else 0
            pred = predict_model(model, data=meteo_scaled)

            corr_diff = pred["prediction_label"]
            corr_sum = recent_sum + pd.Series(corr_diff).cumsum()
            pred_rows = []
            for row in range(len(pred)):
                pred_rows.append({
                    "Datetime": Datetime[row],
                    "model_name": model_name,
                    "corr_diff": corr_diff.iloc[row],
                    "corr_sum": corr_sum.iloc[row],
                    "temp_out": meteo_df["temp_out"].iloc[row],
                    "dew_out": meteo_df["dew_out"].iloc[row],
                    "winddir": meteo_df["winddir"].iloc[row],
                    "qnh": meteo_df["qnh"].iloc[row],
                    "windspeed": meteo_df["windspeed"].iloc[row],
                    "hum_out": meteo_df["hum_out"].iloc[row],
                    "is_forecast": forecast_status[row]
                })
            
            await insert_prediction(pred_rows, overwrite=overwrite)

    except Exception as e:
        return HTMLResponse(
            content=f"Error: {e}, line: {e.__traceback__.tb_lineno}",
            status_code=500
        )


@app.post("/meteo_prediction/missing")
async def predict_missing():
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT m."Datetime"
                FROM meteo m
                LEFT JOIN meteo_prediction p ON m."Datetime" = p."Datetime"
                WHERE p."Datetime" IS NULL
            """)

        rows = sorted(rows, key=lambda x: x["Datetime"])
        missing_preds = [row["Datetime"] for row in rows]
        await predict_and_insert(missing_preds)

        if missing_preds:
            print("Prošlo to predict_missing")

        return {"status": "ok", "predicted": len(missing_preds)}

    except Exception as e:
        return HTMLResponse(
            content=f"Error: {e}, line: {e.__traceback__.tb_lineno}",
            status_code=500
        )
    
@app.post("/meteo_prediction/all_new")
async def predict_all_new():
    try:
        rows = await get_all_meteo()

        rows = sorted(rows, key=lambda x: x["Datetime"])
        preds = [row["Datetime"] for row in rows]
        await predict_and_insert(preds, overwrite=True)

        if preds:
            print("Prošlo to predict_all_new")

        return {"status": "ok", "predicted": len(preds)}

    except Exception as e:
        return HTMLResponse(
            content=f"Error: {e}, line: {e.__traceback__.tb_lineno}",
            status_code=500
        )





scheduler = BackgroundScheduler()
scheduler.add_job(print, "interval", seconds=60, args = ["funguji"])  # Testovací úloha pro ověření, že scheduler funguje
scheduler.add_job(fetch_historical_meteo, "cron", hour=0, args = [1])
scheduler.add_job(fetch_forecast_meteo, "cron", hour=0, minute=5)
scheduler.add_job(predict_missing, "cron", hour=0, minute=10)
scheduler.start()


# Spuštění aplikace
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)

