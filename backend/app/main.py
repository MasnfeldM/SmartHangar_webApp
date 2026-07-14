# -*- coding: utf-8 -*-

from curses import raw
import io
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
from fastapi.responses import JSONResponse, HTMLResponse, Response, FileResponse
import uvicorn
import requests
import os
from sklearn.preprocessing import MinMaxScaler
import asyncpg
from app.database import init_pool, init_schema, get_pool, close_pool
from fastapi import Depends
from contextlib import asynccontextmanager
from app.crud import *
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import httpx
from zoneinfo import ZoneInfo
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR


DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
HISTORY_DATA_API_URL = os.getenv("HISTORY_DATA_API_URL")
FORECAST_DATA_API_URL = os.getenv("FORECAST_DATA_API_URL")
KBELY = os.getenv("KBELY")
station_id, lat, lon, altitude = map(str, KBELY.split(","))
lat, lon, altitude = float(lat), float(lon), int(altitude)
FORECAST_HEADER = dict(item.split(": ") for item in os.getenv("FORECAST_HEADER").split(","))


scheduler = AsyncIOScheduler(timezone=ZoneInfo("Europe/Prague"))

@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    await init_pool(
        host=DB_HOST,
        port=5432,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )
    await init_schema()
    
    scheduler.add_job(print, "interval", hours=1, args = ["funguji"])  # Testovací úloha pro ověření, že scheduler funguje
    scheduler.add_job(fetch_historical_meteo, "cron", hour=0, args = [1])
    scheduler.add_job(fetch_forecast_meteo, "cron", hour=0, minute=5)
    scheduler.add_job(predict_missing, "cron", hour=0, minute=10)
    scheduler.start()
    print("Scheduler running:", scheduler.running)
    for job in scheduler.get_jobs():
        print(job.id)
        print(job.trigger)
    yield
    # shutdown
    scheduler.shutdown()
    await close_pool()






app = FastAPI(lifespan=lifespan, title="meteo backend")

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
            model = load_model(str(p.with_suffix('')))
            models[model_name] = model
            print(f"✅ Načten model: {model_name}")
        except Exception as e:
            print(f"❌ Chyba při načítání modelu {p.stem}: {e}")
    return models

meteo_models = load_models(meteo_models_path)


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(BASE_DIR / "static" / "Spaniel_ikona.png")

import asyncio



def listener(event):
    print(
        f"Job {event.job_id} (output={event.retval}) executed at {datetime.now()} exception={event.exception}",
        flush=True
    )

scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

@app.get("/ping")
async def ping():
    return Response(status_code=200)


@app.head("/meteo", include_in_schema=False)
async def meteo_head():
    return Response(status_code=200)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, 
                ):
    try:    
        
        return templates.TemplateResponse("index.html", {
           "request": request 
        })
    except Exception as e:
        return HTMLResponse(
            content=f"Error: {e}, line: {e.__traceback__.tb_lineno}",
            status_code=500
        )   

@app.get("/meteo", response_class=HTMLResponse)
async def meteo_page(
    request: Request,
):
    try:
        return templates.TemplateResponse("meteo.html", {
            "request": request, 
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
        async with httpx.AsyncClient() as client:
            response = await client.get(HISTORY_DATA_API_URL, params=params)
            response.raise_for_status()
            data = response.json()
        
        meteo_rows = pd.DataFrame(columns=[
            "Datetime", "temp_out", "dew_out", "winddir", "qnh", "windspeed", "hum_out", "is_forecast"
        ])
        meteo_rows.set_index("Datetime", inplace=True)
        for row in data:
            datetime_str = datetime.fromisoformat(row["reportTime"].replace("Z", "+00:00"))
            meteo_row = pd.DataFrame({
                "Datetime" : datetime_str,
                "temp_out" : row["temp"],
                "dew_out" : row["dewp"],
                "winddir" : row["wdir"] if row["wdir"] != "VRB" else 0,
                "qnh" : row["altim"],
                "windspeed" : row["wspd"],
                "hum_out" : 0,  # Placeholder, will be calculated
                "is_forecast" : False
            }, index=[datetime_str])
            
            meteo_row["hum_out"] = 100 * (np.exp((17.625 * meteo_row["dew_out"]) / 
                                                   (243.04 + meteo_row["dew_out"])) 
                                          / np.exp((17.625 * meteo_row["temp_out"]) / 
                                                     (243.04 + meteo_row["temp_out"])))
            meteo_rows = pd.concat([meteo_rows, meteo_row]) 

        meteo_rows.sort_index(inplace=True)  # Seřazení podle datu a času
        meteo_rows.reset_index(inplace=True)  # Obnovení indexu pro vkládání do DB
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
            
        meteo_rows = pd.DataFrame(columns=[
            "Datetime", "temp_out", "dew_out", "winddir", "qnh", "windspeed", "hum_out", "is_forecast"
        ])
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

            meteo_rows = pd.concat([meteo_rows, pd.DataFrame([meteo_row])], ignore_index=True)

        await insert_meteo(meteo_rows)
        return {"message": "Forecast saved to PostgreSQL", "rows": len(meteo_rows)}
    except Exception as e:
        return HTMLResponse(
            content=f"Error: {e}, line: {e.__traceback__.tb_lineno}",
            status_code=500
        )

@app.get("/meteo_prediction", response_class=HTMLResponse)
async def prediction_page(
    request: Request,
):
    try:
        all_model_names = await get_model_names()
        return templates.TemplateResponse("pred.html", {
            "request": request,
            "all_model_names": all_model_names,  
        })
    except Exception as e:
        return HTMLResponse(
            content=f"Error: {e}, line: {e.__traceback__.tb_lineno}",
            status_code=500
        )

@app.get("/api/meteo_prediction")
async def api_meteo_prediction(
    offset: int = 0,
    limit: int = 100,
    model_names: list[str] = None
):
    try:
        all_model_names = await get_model_names()
        raw = await get_prediction(
            limit=limit,
            offset=offset,
            model_names=model_names
        )
        models = {}
        datetimes_set = set()
        is_forecast_set = dict()
        for model, records in raw.items():
            models[model] = {}
            for row in records:
                dt = row["Datetime"].isoformat() if row["Datetime"] else None
                if dt not in datetimes_set:
                    datetimes_set.add(dt)
                is_forecast = row["is_forecast"] if "is_forecast" in row else False
                is_forecast_set[dt] = list()
                is_forecast_set[dt].append(is_forecast) if dt in is_forecast_set else is_forecast_set.setdefault(dt, [is_forecast])
                models[model][dt] = {
                    "sum": row["corr_sum"],
                    "diff": row["corr_diff"]
                }
        datetimes = sorted(datetimes_set)
        for dt in is_forecast_set:
            if all(is_forecast_set[dt]):
                is_forecast_set[dt] = True
            elif not all(is_forecast_set[dt]):
                is_forecast_set[dt] = False
            else:
                raise ValueError(f"Nesrovnalost v is_forecast pro datetime {dt}: {is_forecast_set[dt]}")

        total = await get_prediction_count(model_names=model_names)
        total_max = max(total.values()) if total else 0
        return {
            "all_model_names": all_model_names,
            "datetimes": datetimes,
            "data": models,
            "total_count": total_max,
            "total_pages": math.ceil(
                total_max / limit
            ) if total_max > 0 else 1,
            "offset": offset,
            "limit": limit,
            "is_forecast": is_forecast_set
        }
    except Exception as e:
        return HTMLResponse(
            content=f"Error: {e}, line: {e.__traceback__.tb_lineno}",
            status_code=500
        )


async def predict_and_insert(Datetime: list[datetime], overwrite: bool = False):
    try:
        result = await get_meteo_by_datetime(Datetime=Datetime)
        meteo_models = load_models(meteo_models_path)
        if not result:
            return None
        prev_datetime = await get_prev_datetime(Datetime[0])
        meteo_df = pd.DataFrame([{
            "temp_out": row["temp_out"],
            "dew_out": row["dew_out"],
            "winddir": row["winddir"],
            "qnh": row["qnh"],
            "windspeed": row["windspeed"],
            "hum_out": row["hum_out"]
        } for row in result])
        forecast_status = [row["is_forecast"] for row in result]

        print(meteo_models.items())

        last_predictions = await get_prediction_by_datetime(
            Datetime=prev_datetime,
            model_names=list(meteo_models.keys())
            )
        for model_name, model in meteo_models.items():
            last_prediction = last_predictions.get(model_name)
            recent_sum = last_prediction.get("corr_sum", 0) if last_prediction else 0
            pred = predict_model(model, data=meteo_df)

            corr_diff = pred["prediction_label"]*10000
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
                WHERE (NOT EXISTS (
                    SELECT "Datetime" FROM meteo_prediction p
                    WHERE p."Datetime" = m."Datetime"
                )
                AND m."Datetime" IS NOT NULL) OR m."is_forecast" = TRUE
                ORDER BY m."Datetime" ASC
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

@app.get("/export_csv")
async def export_csv():
    try:
        data_meteo = await get_all_meteo()
        data_pred = await get_all_predictions_history()
        df_meteo = pd.DataFrame(data_meteo)
        df_pred = pd.DataFrame(data_pred)
        meteo_buffer = io.StringIO()
        pred_buffer = io.StringIO()
        df_meteo.to_csv(meteo_buffer, index=False)
        df_pred.to_csv(pred_buffer, index=False)
        return {
            "meteo_csv": meteo_buffer.getvalue(),
            "prediction_csv": pred_buffer.getvalue()
        }
    except Exception as e:
        return HTMLResponse(
            content=f"Error: {e}, line: {e.__traceback__.tb_lineno}",
            status_code=500
        )








# Spuštění aplikace
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)

