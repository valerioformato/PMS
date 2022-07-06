import asyncio
import json
import websockets
import time

nJobs = 0


def getNextJob():
    global nJobs
    nJobs += 1
    return {
        "dataset": "some_dataset",
        "user": "vformato",
        "executable": "testScript.sh",
        "exe_args": [f"{nJobs}"],
        "stdin": "",
        "stdout": "test.out",
        "stderr": "test.err",
        "env": {
            "type": "script",
            "file": "setenv.sh"
        },
        "retries": 0,
        "tags": [
            "cnaf",
            "cern"
        ],
        # "input": {
        #     "files": [
        #         {
        #             "protocol": "xrootd",
        #             "file": "pippo.txt",
        #             "source": "root://eosuser.cern.ch///eos/user/v/vformato/test_pilot/input"
        #         }
        #     ]
        # },
        "output": 
            [
                {
                    "tag": "cnaf",
                    "files": [
                        {
                            "protocol": "local",
                            "file": f"test.*",
                            "destination": "path_cnaf"
                        }
                    ]
                },
                {
                    "tag": "cern",
                    "files": [
                        {
                            "protocol": "local",
                            "file": f"test.*",
                            "destination": "path_cern"
                        }
                    ]
                }
            ]
        ,
        "jobName": "thisIsATestJobForOutput"
    }

# 5
async def send_to_orchestrator(websocket, msg):
    await websocket.send(json.dumps(msg))

    response = await websocket.recv()
    print(f"Server replied: {response}")
    return response


token = "2487e01e-0f32-4172-bb9a-8f58b10f9fb2"

async def hello():
    # uri = "ws://localhost:9002"
    uri = "ws://212.189.205.63/server"
    uri_pilot = "ws://localhost:9003"

    taskName = "sometask"
    
    async with websockets.connect(uri) as websocket:
        # print("Invalid command...")
        # myReq = {"command": "puppamilafava", "task": taskName, "token": token}
        # await send_to_orchestrator(websocket, myReq)

        # print("Creating a new task...")
        # myReq = {"command": "createTask", "task": taskName}
        # resp = await send_to_orchestrator(websocket, myReq)
        # token = resp.split(" ")[-1]
        # 
        # print(f"task {taskName} created with token {token}")

        print("Cleaning the task...")
        myReq = {"command": "cleanTask", "task": taskName, "token": token}
        await send_to_orchestrator(websocket, myReq)

        for _ in range(0, 3):
            newJob = getNextJob()

            myReq = {"command": "submitJob", "job": newJob,
                     "task": taskName, "token": token}

            print("Sending job...")
            resp = await send_to_orchestrator(websocket, myReq)
            jhash = resp.split(' ')[-1]


        time.sleep(2)    
        myReq = {
            "command": "findJobs",
            "match": {"user": "vformato"},
            "filter": {"hash":1, "user": 1, "status": 1}
        }
        await send_to_orchestrator(websocket, myReq)

    # async with websockets.connect(uri) as websocket:
    #     print("Removing the task...")
    #     myReq = {"command": "clearTask", "task": taskName, "token": token}
    #     await send_to_orchestrator(websocket, myReq)

asyncio.run(hello())
