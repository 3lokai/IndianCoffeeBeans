# coffee_scraper/common/deepseek_extractor.py
import json
from openai import OpenAI
from config import DEEPSEEK_API_KEY

async def enhance_with_deepseek(coffee, roaster_name):
    """Use DeepSeek to enhance coffee attributes from description."""
    if not coffee.get("description"):
        return coffee
        
    try:
        prompt = f"""Extract coffee attributes from this product description.
        
        Coffee name: {coffee['name']}
        Roaster: {roaster_name}
        Description: {coffee['description'][:1500]}
        
        Please extract ONLY these attributes in JSON format:
        1. roast_level: (one of: light, medium-light, medium, medium-dark, dark)
        2. bean_type: (one of: arabica, robusta, blend)
        3. processing_method: (one of: washed, natural, honey, anaerobic, or null if unclear)
        4. flavor_profiles: (array of flavor notes like: chocolate, fruity, nutty, etc.)
        5. suitable_for: (array of brew methods like: espresso, filter, cold brew, etc.)
        
        Return ONLY the JSON object with these fields and nothing else:"""
        
        client = OpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url="https://api.deepseek.com"
        )
        
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "You are a coffee expert who extracts structured data from descriptions."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500,
            temperature=0.1
        )
        
        ai_response = response.choices[0].message.content
        
        # Extract JSON from the response
        try:
            # Find JSON in the response
            json_start = ai_response.find('{')
            json_end = ai_response.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_str = ai_response[json_start:json_end]
                attributes = json.loads(json_str)
                
                # Update coffee with extracted attributes
                for key, value in attributes.items():
                    if value and (key not in coffee or not coffee[key]):
                        coffee[key] = value
        except Exception as e:
            print(f"Error parsing DeepSeek JSON: {e}")
    
        return coffee
    except Exception as e:
        print(f"Error enhancing with DeepSeek: {e}")
        return coffee