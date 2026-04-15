import os
import asyncpg
from app.database import get_pool
from datetime import datetime



# ==================== METEO ====================

async def insert_meteo(meteo_rows: list[dict]):
    async with get_pool().acquire() as conn:
        try:
            return await conn.executemany("""
                    INSERT INTO meteo ("Datetime", temp_out, dew_out, winddir, qnh, windspeed, hum_out, is_forecast)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT ("Datetime") DO UPDATE SET 
                        temp_out = EXCLUDED.temp_out,
                        dew_out = EXCLUDED.dew_out,
                        winddir = EXCLUDED.winddir,
                        qnh = EXCLUDED.qnh,
                        windspeed = EXCLUDED.windspeed,
                        hum_out = EXCLUDED.hum_out
                    WHERE meteo.is_forecast = TRUE
                """, [(row["Datetime"], row["temp_out"], row["dew_out"], row["winddir"], row["qnh"], row["windspeed"], row["hum_out"], row["is_forecast"]) for row in meteo_rows])
        except Exception as e:
            await conn.execute("ROLLBACK")
            print(f"Chyba při insertu do meteo: {e}")

async def get_meteo(limit: int = 100, offset: int = 0) -> list[asyncpg.Record]:
    try:
        async with get_pool().acquire() as conn:
            return await conn.fetch("SELECT * FROM meteo ORDER BY \"Datetime\" ASC LIMIT $1 OFFSET $2", limit, offset)
            
    except Exception as e:
        print(f"Chyba při čtení z meteo: {e}")
        return []

async def get_meteo_by_datetime(Datetime: list[datetime], offset: int = 0) -> list[asyncpg.Record]:
    try:
        async with get_pool().acquire() as conn:
            return await conn.fetch("SELECT * FROM meteo WHERE \"Datetime\" = ANY($1) ORDER BY \"Datetime\" ASC OFFSET $2", Datetime, offset)
    except Exception as e:
        print(f"Chyba při čtení z meteo podle Datetime: {e}")
        return []

async def get_all_meteo() -> list[asyncpg.Record]:
    try:
        async with get_pool().acquire() as conn:
            return await conn.fetch("SELECT * FROM meteo ORDER BY \"Datetime\" ASC")
    except Exception as e:
        print(f"Chyba při čtení z meteo: {e}")
        return []

async def get_meteo_count() -> int:
    try:
        async with get_pool().acquire() as conn:
            result = await conn.fetchrow("SELECT COUNT(*) AS total FROM meteo")
            return result['total'] if result else 0
    except Exception as e:
        print(f"Chyba při získávání počtu záznamů v meteo: {e}")
        return 0

async def update_meteo(Datetime: datetime, temp_out: float = None, dew_out: float = None, 
                       winddir: float = None, qnh: float = None, windspeed: float = None, 
                       hum_out: float = None):
    fields = []
    values = []
    if temp_out   is not None: fields.append("temp_out = $1");   values.append(temp_out)
    if dew_out    is not None: fields.append("dew_out = $2");    values.append(dew_out)
    if winddir    is not None: fields.append("winddir = $3");    values.append(winddir)
    if qnh        is not None: fields.append("qnh = $4");        values.append(qnh)
    if windspeed  is not None: fields.append("windspeed = $5");  values.append(windspeed)
    if hum_out    is not None: fields.append("hum_out = $6");    values.append(hum_out)

    if not fields:
        return

    values.append(Datetime)
    try:
        async with get_pool().acquire() as conn:
            await conn.execute(f"UPDATE meteo SET {', '.join(fields)} WHERE \"Datetime\" = $7", values)
    except Exception as e:
        conn.rollback()
        print(f"Chyba při update meteo: {e}")

async def delete_meteo(limit: int = 100, offset: int = 0):
    try:
        async with get_pool().acquire() as conn:
            await conn.execute("DELETE FROM meteo ORDER BY \"Datetime\" ASC LIMIT $1 OFFSET $2", limit, offset)
    except Exception as e:
        conn.rollback()
        print(f"Chyba při delete z meteo: {e}")


# ==================== METEO PREDICTION ====================

async def insert_prediction(pred_rows: list[dict], overwrite: bool = False):
    try:
        async with get_pool().acquire() as conn:
            forecast_at = datetime.now()
            if overwrite:
                await conn.executemany("""
                    INSERT INTO meteo_prediction_history (forecast_at, "Datetime",
                                model_name, corr_diff, corr_sum, temp_out, 
                                dew_out,winddir, qnh, windspeed, hum_out)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,  [(forecast_at, row['Datetime'], row['model_name'], row['corr_diff'], row['corr_sum'], 
                        row['temp_out'], row['dew_out'], row['winddir'],
                        row['qnh'], row['windspeed'], row['hum_out']) for row in pred_rows])
                result = await conn.executemany("""
                    INSERT INTO meteo_prediction ("Datetime", model_name, corr_diff, corr_sum, from_forecast)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT ("Datetime", model_name) DO UPDATE SET 
                        corr_diff = EXCLUDED.corr_diff,
                        corr_sum = EXCLUDED.corr_sum
                    """, [(row['Datetime'], row['model_name'], row['corr_diff'], row['corr_sum'], row['is_forecast']) for row in pred_rows])
            else:
                await conn.executemany("""
                    INSERT INTO meteo_prediction_history (forecast_at, "Datetime", 
                                model_name, corr_diff, corr_sum, temp_out, 
                                dew_out,winddir, qnh, windspeed, hum_out)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,  [(forecast_at, row['Datetime'], row['model_name'], row['corr_diff'], row['corr_sum'], 
                    row['temp_out'], row['dew_out'], row['winddir'], 
                    row['qnh'], row['windspeed'], row['hum_out']) for row in pred_rows])
                result = await conn.executemany("""
                    INSERT INTO meteo_prediction ("Datetime", model_name, corr_diff, corr_sum, from_forecast)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT ("Datetime", model_name) DO UPDATE SET 
                        corr_diff = EXCLUDED.corr_diff, 
                        corr_sum = EXCLUDED.corr_sum
                    WHERE meteo_prediction.from_forecast = TRUE
                """, [(row['Datetime'], row['model_name'], row['corr_diff'], row['corr_sum'], row['is_forecast']) for row in pred_rows])
            print(f"Result of insert_prediction: {result}")
    except Exception as e:
        conn.rollback()
        print(f"Chyba při insertu do meteo_prediction: {e}")
        raise e

async def get_prediction(limit: int = 100, 
                         offset: int = 0, 
                         model_names: list[str] = None) -> dict[str, list[dict[str, any]]]:
    try:
        async with get_pool().acquire() as conn:
            if model_names:
                placeholders = ', '.join(['$' + str(i+1) for i in range(len(model_names))])
                result = await conn.fetch(f"""
                    SELECT * FROM meteo_prediction
                    WHERE model_name IN ({placeholders})
                    ORDER BY "Datetime" ASC
                    LIMIT ${{len(model_names) + 1}} OFFSET ${{len(model_names) + 2}}
                """, *model_names, limit, offset)
                result = [dict(record) for record in result]
                return {model_name: [record for record in result if record['model_name'] == model_name] for model_name in model_names}
            else:
                result = await conn.fetch("""
                    SELECT * FROM meteo_prediction
                    ORDER BY "Datetime" ASC
                    LIMIT $1 OFFSET $2
                """, limit, offset)
                result = [dict(record) for record in result]
                return {str(None): result}
    except Exception as e:
        print(f"Chyba při čtení z meteo_prediction: {e}")
        return {str(None): []}
    
async def get_model_names() -> list[str]:
    try:
        async with get_pool().acquire() as conn:
            records = await conn.fetch("SELECT DISTINCT model_name FROM meteo_prediction")
            return [record['model_name'] for record in records]
    except Exception as e:
        print(f"Chyba při získávání model names z meteo_prediction: {e}")
        return []
                
async def get_prediction_by_datetime(Datetime: datetime, 
                                     model_names: list[str], 
                                     offset: int = 0) -> dict[str, dict[str, any]]:
    try:
        async with get_pool().acquire() as conn:
            placeholders = ', '.join(['$' + str(i+1) for i in range(len(model_names))])
            result = await conn.fetchrow(f"""
                SELECT * FROM meteo_prediction
                WHERE "Datetime" = $1 AND model_name IN ({placeholders}) OFFSET ${{len(model_names) + 2}}
            """, Datetime, *model_names, offset)
            return {model_name: dict(result) for model_name in model_names}
    except Exception as e:
        print(f"Chyba při čtení z meteo_prediction podle Datetime: {e}")
        return {str(None): {}}

async def get_all_predictions(model_names: list[str] = None) -> dict[str, list[dict[str, any]]]:
    try:
        if model_names:
            async with get_pool().acquire() as conn:
                placeholders = ', '.join(['$' + str(i+1) for i in range(len(model_names))])
                result = await conn.fetch(f"""
                    SELECT * FROM meteo_prediction
                    WHERE model_name IN ({placeholders})
                    ORDER BY "Datetime" ASC
                """, *model_names)
                result = [dict(record) for record in result]
            return {model_name: [record for record in result 
                                 if record['model_name'] == model_name]
                        for model_name in model_names}

        else:
            async with get_pool().acquire() as conn:
                result = await conn.fetch("""
                    SELECT * FROM meteo_prediction
                    ORDER BY "Datetime" ASC
                """)
            return {"vše": [dict(record) for record in result]}
    except Exception as e:
        print(f"Chyba při čtení z meteo_prediction: {e}")
        return {str(None): []}
    
async def get_prediction_count(model_names: list[str] = None) -> dict[str, int]:
    try:
        async with get_pool().acquire() as conn:
            if model_names:
                result = [await conn.fetchrow(f"""
                    SELECT COUNT(*) FROM meteo_prediction
                    WHERE model_name IS $1
                """, model_name) for model_name in model_names]
                return {model_name: record['count'] for model_name, record in zip(model_names, result)}
            else:
                result = await conn.fetchrow("SELECT COUNT(*) FROM meteo_prediction")
                return {"vše": result['count']}
    except Exception as e:
        print(f"Chyba při získávání počtu záznamů v meteo_prediction: {e}")
        return {model_name: 0 for model_name in model_names} if model_names else {str(None): 0}

async def update_prediction(Datetime: datetime, model_name: str, corr_diff: float = None, corr_sum: float = None):
    fields = []
    values = []
    if corr_diff is not None: fields.append("corr_diff = $1"); values.append(corr_diff)
    if corr_sum  is not None: fields.append("corr_sum = $2");  values.append(corr_sum)

    if not fields:
        return

    values.extend([Datetime, model_name])
    try:
        async with get_pool().acquire() as conn:
            await conn.execute(f"""
                UPDATE meteo_prediction SET {', '.join(fields)}
                WHERE "Datetime" = $3 AND model_name = $4
            """, *values)
    except Exception as e:
        print(f"Chyba při update meteo_prediction: {e}")

async def delete_prediction(limit: int = 100, offset: int = 0, model_name: str = None):
    try:
        if model_name:
            async with get_pool().acquire() as conn:
                await conn.execute("""
                    DELETE FROM meteo_prediction ORDER BY "Datetime" ASC
                    WHERE model_name = $1
            """, model_name)
        else:
            async with get_pool().acquire() as conn:
                await conn.execute("""
                    DELETE FROM meteo_prediction ORDER BY "Datetime" ASC LIMIT $1 OFFSET $2
                """, limit, offset)
    except Exception as e:
        print(f"Chyba při delete z meteo_prediction: {e}")
