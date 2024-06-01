from pydantic import BaseModel
from typing import List

class datasetOption(BaseModel):
    option: List[str] = []
    
class datasetSelection(BaseModel):
    bucket_name : str
    measurement : str

