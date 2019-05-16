from src.data.future.hq import insert_hq_to_mongo, build_future_index
from src.data.future.spread import insert_spot_to_mongo
from src.features import build_all_blocks

if __name__ == '__main__':
    insert_hq_to_mongo()
    build_future_index()
    build_all_blocks()
    insert_spot_to_mongo()
