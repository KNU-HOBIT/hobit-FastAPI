import requests

def data_by_time_range_req(start, end, bucket, measurement, tag_key, tag_value, send_topic: str):
    url = f"http://155.230.36.25:3001/data-by-time-range/?start={start}&end={end}&bucket={bucket}&measurement={measurement}&tag_key={tag_key}&tag_value={tag_value}&send_topic={send_topic}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Will raise an exception for HTTP error responses
        return response.json()
    except requests.RequestException as e:
        # Logs the exception details including method and url
        print(f"HTTP request failed: {e}, URL: {url}")
        return None, None, None
    

def all_of_data_req(bucket, measurement, tag_key, tag_value, send_topic: str):
    url = f"http://155.230.36.25:3001/all-of-data/?bucket={bucket}&measurement={measurement}&tag_key={tag_key}&tag_value={tag_value}&send_topic={send_topic}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Will raise an exception for HTTP error responses
        return response.json()
    except requests.RequestException as e:
        # Logs the exception details including method and url
        print(f"HTTP request failed: {e}, URL: {url}")
        return None, None, None
    

    


