import os
import json

#db생성
def create_db(db_info):
    db_name = db_info["db_name"]

    # 디렉토리 생성
    os.makedirs(f'./{db_name}/tables', exist_ok=True)
    os.makedirs(f'./{db_name}/archive', exist_ok=True)

    # info.json 작성
    with open(f"./{db_name}/info.json", "w", encoding="utf-8") as f:
        json.dump(db_info, f, indent=4, ensure_ascii=False)

    print(f"DB '{db_name}' created successfully!\n")

#db삭제
def remove_db(db_name, db_info) :
    pass