import json
import pandas as pd

def extract_from_json(file_path):
    extracted_data = []
    tweet_ids = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                try:
                    data = json.loads(line)
                    
                    # Check for duplicate tweets based on tweet_id
                    tweet_id = data.get('id_str')
                    if tweet_id in tweet_ids:
                        continue
                    tweet_ids.add(tweet_id)
                    
                    extracted_data.append(data)
                
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON in {file_path}: {e}")
                    continue
        
        # Convert extracted_data list to DataFrame
        df = pd.DataFrame(extracted_data)
        return df
    
    except FileNotFoundError:
        print(f"Error: The file {file_path} does not exist.")
        return None
