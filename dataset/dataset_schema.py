from pydantic import BaseModel
from typing import List

class datasetOption(BaseModel):
    option: List[str] = []
    
class datasetSelection(BaseModel):
    bucket_name : str
    measurement : str


# 예시로 사용할 수 있는 코드
#dataset1 = DatasetOption(option_name="example_name")
#dataset2 = DatasetOption(option_name="example_name", option_value=["나이", "이름"])
#dataset3 = DatasetOption(option_name="example_name", option_value=["나이", "이름", "주소"])

# 출력
#print(dataset1)  # option_value는 빈 리스트로 설정됨
#print(dataset2)  # option_value는 ["나이", "이름"]로 설정됨
#print(dataset3)  # option_value는 ["나이", "이름", "주소"]로 설정됨

