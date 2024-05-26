# from sqlalchemy import *
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.declarative import declarative_base
# DB_URL = 'mysql+pymysql://root:knulinkmoa1234@localhost:3306/hobit'


# engine = create_engine(DB_URL,pool_size=50)
# SessionLocal = sessionmaker(autocommit=False,autoflush=False,bind=engine)

# Base=declarative_base

# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()
