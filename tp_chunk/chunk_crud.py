from sqlalchemy.orm import Session
from datetime import datetime

from models import Chunk
from tp_chunk.chunk_schema import ChunkReadReq
from request.request_crud import check_elapsed_req
from spark.spark_init import chunk_spark
import spark.util as util
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
        
        if req.measurement == "transport": proto_msg_type = "Transport"
        elif req.measurement == "electric_dataset": proto_msg_type = "Electric"
        else: 
            print("Can't found PROTOBUF MESSAGE TYPE")
            return
            
        chunk_job_arg = \
        { 
            "start_time" : responce_result.get("startTimeMillis", None),
            "end_time" : responce_result.get("endTimeMillis", None), 
            "topic" : responce_result.get("sendTopicStr", None), 
            "proto_message_type": proto_msg_type,
        }
        
        collect = util.process_spark_tasks(
            chunk_spark.get_spark()[0],
            chunk_spark.get_spark()[1], 
            'PROCESS_CHUNK',
            chunk_job_arg, )
        
        for row in collect:
            new_chunk = Chunk(
                bucket=req.bucket,
                measurement=req.measurement,
                tagKey=req.tag_key,
                tagValue=req.tag_value,
                startTs=row.start_timestamp_unix,
                endTs=row.end_timestamp_unix,
                chunkDuration=row.chunk_duration,
                count=row['count']
            )
            db.add(new_chunk)
        db.commit()

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
        # 2.3.1.    ets - last_chuck_ts <= 10분   ||> 조회한 모든 chunk 정보 return.
        if ets - last_chunk_end > 600000: # 
            # 2.3.2.    ets - last_chuck_ts > 10분  ||> last_chuck_ts ~ ets 범위로 make_chunk_in_elapsed 함수를 실행.
            responce_result = data_by_time_range_req(
                start=last_chunk_end, 
                end=ets, 
                bucket=req.bucket, 
                measurement=req.measurement, 
                tag_key=req.tag_key, 
                tag_value=req.tag_value 
            )
            
            if req.measurement == "transport": proto_msg_type = "Transport"
            elif req.measurement == "electric_dataset": proto_msg_type = "Electric"
            else: 
                print("Can't found PROTOBUF MESSAGE TYPE")
                return
            
            
            chunk_job_arg = \
            { 
                "start_time" : responce_result.get("startTimeMillis", None),
                "end_time" : responce_result.get("endTimeMillis", None), 
                "topic" : responce_result.get("sendTopicStr", None), 
                "proto_message_type": proto_msg_type,
            }
            
            collect = util.process_spark_tasks(
                chunk_spark.get_spark()[0],
                chunk_spark.get_spark()[1], 
                'PROCESS_CHUNK',
                chunk_job_arg, )

            for row in collect:
                new_chunk = Chunk(
                    bucket=req.bucket,
                    measurement=req.measurement,
                    tagKey=req.tag_key,
                    tagValue=req.tag_value,
                    startTs=row.start_timestamp_unix,
                    endTs=row.end_timestamp_unix,
                    chunkDuration=row.chunk_duration,
                    count=row['count']
                )
                db.add(new_chunk)
            db.commit()
            
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


