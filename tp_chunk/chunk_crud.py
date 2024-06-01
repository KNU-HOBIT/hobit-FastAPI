from sqlalchemy.orm import Session
from datetime import datetime

from models import Chunk
from tp_chunk.chunk_schema import ChunkReadReq
from request.request_crud import check_elapsed_req
from spark.spark_crud import make_chunk_in_elapsed
import spark.spark_init as spark_initiator
from request.request_crud import data_by_time_range_req, all_of_data_req


def list_all_chunk(req :ChunkReadReq, db:Session):

    def convert_to_unix_microseconds(timestamp_str: str) -> int:
        # Parse the string to a datetime object
        dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        # Convert to Unix timestamp and return microseconds
        return int(dt.timestamp() * 1000)

    # 0. 요청하는 데이터의 start / end TS를 IoT서버에 Query.
    res = check_elapsed_req(req.bucket, req.measurement, req.tag_key, req.tag_value)
    sts = convert_to_unix_microseconds(res.get("queryStartStr"))
    ets = convert_to_unix_microseconds(res.get("queryEndStr"))

    # 1.        일단 DB에 sts ~ ets 청크 쿼리를 날려본다.
    existing_chunks = db.query(Chunk).filter(
        Chunk.bucket == req.bucket,
        Chunk.measurement == req.measurement,
        Chunk.tagKey == req.tag_key,
        Chunk.tagValue == req.tag_value,
        Chunk.startTs >= sts,
        Chunk.endTs <= ets
    ).all()

    if not existing_chunks:
        # 2.1.      한 개도 없으면, sts ~ ets 범위로 make_chunk_in_elapsed 함수를 실행.
        print("No chunks found, generate for the entire range")
        responce_result = all_of_data_req( 
            bucket=req.bucket, 
            measurement=req.measurement, 
            tag_key=req.tag_key, 
            tag_value=req.tag_value 
            )

        # Generate chunks
        make_chunk_in_elapsed( 
            spark=spark_initiator.get_spark(), db=db, bucket=req.bucket,
            measurement=req.measurement, 
            tag_key=req.tag_key, tag_value=req.tag_value, 
            start_time=responce_result.get("startTimeMillis", None), 
            end_time=responce_result.get("endTimeMillis", None) 
            )

        existing_chunks = db.query(Chunk).filter(
            Chunk.bucket == req.bucket,
            Chunk.measurement == req.measurement,
            Chunk.tagKey == req.tag_key,
            Chunk.tagValue == req.tag_value,
            Chunk.startTs >= sts,
            Chunk.endTs <= ets
        ).all()
    else:
        #  2.2.     chuck가 1개 이상이라면, 조회된 모든 chunk 중 가장 최근 chunk의 end_Ts를 last_chuck_ts라고 정의 
        print("Check if there's a gap in the chunks")
        last_chunk_end = max(chunk.endTs for chunk in existing_chunks)
        # 2.3.1.    last_chuck_ts + 10분 >= ets ||> 조회한 모든 chunk 정보 return.
        if last_chunk_end + 600000 < ets: # 
            # 2.3.2.    last_chuck_ts + 10분  < ets  ||> last_chuck_ts ~ ets 범위로 make_chunk_in_elapsed 함수를 실행.
            responce_result = data_by_time_range_req(start=last_chunk_end, 
                                                     end=ets, 
                                                     bucket=req.bucket, 
                                                     measurement=req.measurement, 
                                                     tag_key=req.tag_key, 
                                                     tag_value=req.tag_value )
            
            # Generate chunks for the missing range
            make_chunk_in_elapsed(
                spark=spark_initiator.get_spark(), db=db, bucket=req.bucket,
                measurement=req.measurement, 
                tag_key=req.tag_key, tag_value=req.tag_value, 
                start_time=responce_result.get("startTimeMillis", None), 
                end_time=responce_result.get("endTimeMillis", None) 
                )
            
            existing_chunks = db.query(Chunk).filter(
                Chunk.bucket == req.bucket,
                Chunk.measurement == req.measurement,
                Chunk.tagKey == req.tag_key,
                Chunk.tagValue == req.tag_value,
                Chunk.startTs >= sts,
                Chunk.endTs <= ets
            ).all()

    # For debugging purposes, we print the result instead of returning it
    result = 0
    for c in existing_chunks:
        # c.print_chunk()
        result += c.chunkDuration
    print(req.tag_key, req.tag_value, "누적운행량 : ", result)
    return result


