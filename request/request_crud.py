import requests

def get_request_data(start: str, end: str, bucket: str, eqp_id: int, send_topic: str):
    url = f"http://155.230.36.25:3001/data-by-time-range/?bucket={bucket}&start={start}&end={end}&eqp_id={eqp_id}&send_topic={send_topic}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Will raise an exception for HTTP error responses
        data = response.json()
        print(data)
        return (
            data.get("queryStartStr", "queryStartStr not available"),
            data.get("queryEndStr", "queryEndStr not available"),
            data.get("eqpIdStr", "eqpIdStr not available"),
            data.get("bucketStr", "bucketStr not available"),
            data.get("sendTopicStr", "sendTopicStr not available"),
            data.get("totalMessages", "totalMessages not available"),
            data.get("startTimeMillis", "startTime not available"),
            data.get("endTimeMillis", "endTime not available")
        )
    except requests.RequestException as e:
        # Logs the exception details including method and url
        print(f"HTTP request failed: {e}, URL: {url}")
        return None, None, None
    

    

