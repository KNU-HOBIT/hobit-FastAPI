from pydantic import BaseModel
from typing import List

class datasetOption(BaseModel):
    option: List[str] = []
    
class datasetSelection(BaseModel):
    bucket_name : str
    measurement : str

class MLList(BaseModel):
    bucket = str
    measurement = str
    tagKey = str
    tagValue = str
    mlStart = int
    mlEnd = int
    
class getML(BaseModel):
    mlStart =int
