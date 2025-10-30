import pandas as pd

def generate_response(intent_data, results):
    """
    Convert raw query results into a user-friendly textual summary.
    """
    intent = intent_data.get("intent", "")
    entities = intent_data.get("entities", {})
    district = entities.get("district", "")
    crop = entities.get("crop", "")
    season = entities.get("season", "")

    if results is None or results.empty:
        return f"No matching data found for {district or 'your query'}."

    # Normalize column names (handle spaces and case)
    results.columns = [c.strip().replace(" ", "_").lower() for c in results.columns]

    if intent == "crop_production":
        if "crop" in results.columns and "production" in results.columns:
            top_crops = results.head(5)
            summary = "\n".join([f"- {r['crop']}: {r['production']}" for _, r in top_crops.iterrows()])
            return f"🌾 Top crops in {district} during {season} season:\n{summary}"
        else:
            return "⚠️ Missing crop production columns in dataset."

    elif intent == "rainfall":
        if "rainfall" in results.columns:
            avg = results["rainfall"].mean()
            return f"🌦️ Average rainfall in {district}: {avg:.2f} mm."
        else:
            return "⚠️ Rainfall data not found in dataset."

    elif intent == "market_price":
        # Ensure modal price column exists (handle variations)
        modal_col = next((c for c in results.columns if "modal" in c and "price" in c), None)
        if not modal_col:
            return "⚠️ Could not find the 'Modal Price' column in your Market Prices dataset."

        results_sorted = results.sort_values(by=modal_col, ascending=False).head(5)
        summary = "\n".join([
            f"- {r.get('commodity', 'Unknown')}: ₹{r[modal_col]:,.2f} (Market: {r.get('market', 'N/A')})"
            for _, r in results_sorted.iterrows()
        ])
        return f"💹 Market prices in {district or 'the region'}:\n{summary}"

    elif intent == "groundwater":
        if "depth_to_water_level" in results.columns:
            avg = results["depth_to_water_level"].mean()
            return f"💧 Average groundwater level in {district}: {avg:.2f} meters."
        else:
            return "⚠️ Groundwater column not found in dataset."

    else:
        return "❓ I’m not sure how to answer that yet."
