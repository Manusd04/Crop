# intelligent_qna/query_parser.py
import re

def parse_query(question: str):
    """
    Parses a natural language question and extracts intent, entities and parameters.
    """
    question_lower = question.lower()

    # detect district/state/crop names
    district = re.findall(r"(?:in|for)\s+([a-zA-Z\s]+?)(?:\s+during|\s*$)", question_lower)
    crop = re.findall(r"(?:of|for)\s+([a-zA-Z\s]+?)(?:\s+during|\s+in|\s*$)", question_lower)
    season = re.findall(r"(?:during|in)\s+(kharif|rabi|summer|winter)", question_lower)

    entities = {
        "district": district[0].strip().title() if district else None,
        "crop": crop[0].strip().title() if crop else None,
        "season": season[0].title() if season else None
    }

    # detect type of intent
    if "top crop" in question_lower or "most produced" in question_lower:
        intent = "crop_production"
    elif "rainfall" in question_lower or "rain" in question_lower:
        intent = "rainfall"
    elif "price" in question_lower or "market" in question_lower:
        intent = "market_price"
    elif "ground water" in question_lower or "groundwater" in question_lower:
        intent = "groundwater"
    else:
        intent = "unknown"

    return {"intent": intent, "entities": entities}
