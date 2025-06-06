# utils/google_places.py
import requests
import os

# Function for Google Places API call (without Streamlit dependencies)
def get_google_places_suggestions_backend(query, api_key, types='locality'):
    if not query or not api_key:
        return []
    url = "https://places.googleapis.com/v1/places:autocomplete"
    headers = {"Content-Type": "application/json", 'X-Goog-Api-Key': api_key}
    data_payload = {
        "input": query,
        "includedPrimaryTypes": [types] if isinstance(types, str) else types,
    }
    try:
        response = requests.post(url, headers=headers, json=data_payload)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        
        # Extract mainText and secondaryText as desired
        suggestions = []
        for item in response.json().get('suggestions', []):
            if 'placePrediction' in item and 'structuredFormat' in item['placePrediction']:
                main_text = item['placePrediction']['structuredFormat']['mainText']['text']
                secondary_text = item['placePrediction']['structuredFormat']['secondaryText']['text']
                suggestions.append(f"{main_text}, {secondary_text}")
        return suggestions
    except requests.exceptions.RequestException as e:
        print(f"Google Places API Error: {e}") # Log error, don't use st.error
        return []