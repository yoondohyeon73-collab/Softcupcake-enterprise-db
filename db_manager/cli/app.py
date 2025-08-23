import os
import json
import functions

#메뉴얼 출력
print_arg = '''

Softcupcake Enterprise Database
Copyright © 2025 윤도현. All rights reserved.

--------------------------------------------

Command Manual

When creating a table 
-> Enter 'create' and follow the command
If you want to run the DB
-> Enter 'serve' and follow the command
If you want to end the cli
-> Enter 'exit'

'''
print(print_arg)

# DB 버전 정보
DB_VERSION = {
    "version": "1.0",
    "is_beta": False
}

while True:
    user_cmd = input("CMD : ").strip().lower()

    # DB 생성 명령
    if user_cmd == "create":
        db_name = input("Enter db name : ").strip()
        server_port = int(input("Enter the port to run the server : ").strip())

        # DB 정보 딕셔너리
        db_info = {
            "db_name": db_name,
            "server_port": server_port,
            "version": DB_VERSION["version"],
            "is_beta": DB_VERSION["is_beta"]
        }

        # 확인 출력
        print("\nCheck DB information\n")
        print(json.dumps(db_info, indent=4))
        confirm = input("\nCreate DB? (y/n): ").strip().lower()

        if confirm == "y":
            functions.create_db(db_info)
        else:
            print("DB creation canceled.\n")

    # db삭제 명령
    elif user_cmd == "remove":
        pass

    #cli종료 명령
    elif user_cmd == "exit":
        print("End program.")
        break

    else:
        print("Unknown command.\n")