# intelligent_qna/knowledge_engine.py
import pandas as pd

def query_datasets(intent_data, datasets):
    """
    Based on intent, select and filter appropriate dataset(s)
    """
    intent = intent_data["intent"]
    entities = intent_data["entities"]
    district = entities.get("district")
    crop = entities.get("crop")
    season = entities.get("season")

    if intent == "crop_production":
        df = datasets["crop"]
        mask = (df["District"].str.contains(district, case=False, na=False)) if district else True
        result = df[mask].groupby("Crop")["Production"].sum().reset_index().sort_values(by="Production", ascending=False).head(5)
        return result

    elif intent == "rainfall":
        df = datasets["rain"]
        result = df[df["District"].str.contains(district, case=False, na=False)] if district else df
        return result

    elif intent == "market_price":
        df = datasets["market"]
        result = df[df["District"].str.contains(district, case=False, na=False)] if district else df
        if crop:
            result = result[result["Commodity"].str.contains(crop, case=False, na=False)]
        return result.head(10)

    elif intent == "groundwater":
        df = datasets["groundwater"]
        result = df[df["District"].str.contains(district, case=False, na=False)] if district else df
        return result

    else:
        return pd.DataFrame()
