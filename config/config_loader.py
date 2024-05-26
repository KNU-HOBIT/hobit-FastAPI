import os
import yaml

# 전역 변수로 설정 저장
global_config = None

def load_config():
    global global_config
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml')
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
    with open(config_path, 'r') as yaml_conf:
        global_config = yaml.safe_load(yaml_conf)
    return global_config

# 모듈 로드 시 자동으로 설정 로드
load_config()

def get_config():
    return global_config
