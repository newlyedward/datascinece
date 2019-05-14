from src.data.future.hq import insert_hq_to_mongo, build_future_index
from src.features.block.block import build_segments

if __name__ == '__main__':
    insert_hq_to_mongo()
    build_future_index()
    build_segments()
