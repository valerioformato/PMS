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
        "jobName": "thisIsATestJobForOutput"
    }


async def send_to_orchestrator(websocket, msg):
    await websocket.send(json.dumps(msg))

    response = await websocket.recv()
    print(f"Server replied: {response}")
    return response


token = "d06dfaba-9e30-412d-8d9a-eb765351a19b"


async def hello():
    uri = "ws://localhost:9002"

    taskName = "sometask"

    async with websockets.connect(uri) as websocket:
        print("Invalid command...")
        myReq = {"command": "puppamilafava", "task": taskName, "token": token}
        await send_to_orchestrator(websocket, myReq)

        print("Cleaning the task...")
        myReq = {"command": "cleanTask", "task": taskName, "token": token}
        await send_to_orchestrator(websocket, myReq)

        print("Creating a new task...")
        myReq = {"command": "createTask", "task": taskName}
        resp = await send_to_orchestrator(websocket, myReq)
        newToken = resp.split(" ")[-1]

        print(f"task {taskName} created with token {newToken}")

        for _ in range(0, 3):
            newJob = getNextJob()

            myReq = {"command": "submitJob", "job": newJob,
                     "task": taskName, "token": newToken}

            print("Sending job...")
            await send_to_orchestrator(websocket, myReq)

asyncio.run(hello())

# async def simulate_pilot():
#     uri = "ws://localhost:9003"

#     async with websockets.connect(uri) as websocket:
#         print("Sending request...")
#         myReq = {
#             "command": "p_claimJob",
#             "filter": {"task": "sometask"},
#             "pilotUuid": "unnumeroacaso",
#             "token": token,
#         }

#         job = json.loads(await send_to_orchestrator(websocket, myReq))
#         print(json.dumps(job, indent=2))

#         myReq = {
#             "command": "p_updateJobStatus",
#             "status": "Running",
#             "hash": job["hash"],
#             "task": job["task"],
#             "token": token,
#         }
#         await send_to_orchestrator(websocket, myReq)

#         time.sleep(1)
#         myReq["status"] = "Done"
#         await send_to_orchestrator(websocket, myReq)
