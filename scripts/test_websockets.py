import asyncio
import json
import websockets
import time

nJobs = 0


def getNextJob():
    global nJobs
    nJobs += 1
    return {
        "task": "sometask",
        "subtask": "somesubtask",
        "dataset": "some_dataset",
        "user": "vformato",
        "executable": "testScript.sh",
        "exe_args": [f"{nJobs}"],
        # "executable": "ls",
        # "exe_args": [],
        "stdin": "",
        "stdout": "test.out",
        "stderr": "test.err",
        "env": {
            "type": "script",
            "file": "setenv.sh"
        },
        "retries": 0,
        "sites": [
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


token = "1cf2ebae-be4a-47df-a850-1157a6e5aeae"


async def hello():
    uri = "ws://localhost:9002"

    async with websockets.connect(uri) as websocket:
        print("Cleaning the task...")
        myReq = {"command": "cleanTask", "task": "sometask", "token": token}
        await send_to_orchestrator(websocket, myReq)

        print("Creating a new task...")
        myReq = {"command": "createTask", "task": "sometask"}
        resp = await send_to_orchestrator(websocket, myReq)
        newToken = resp.split(" ")[-1]

        for ijob in range(0, 3):
            newJob = getNextJob()

            myReq = {"command": "submitJob", "job": newJob,
                     "task": "sometask", "token": newToken}

            print("Sending a valid JSON job...")
            await send_to_orchestrator(websocket, myReq)


async def simulate_pilot():
    uri = "ws://localhost:9003"

    async with websockets.connect(uri) as websocket:
        print("Sending request...")
        myReq = {
            "command": "p_claimJob",
            "filter": {"task": "sometask"},
            "pilotUuid": "unnumeroacaso",
            "token": token,
        }

        job = json.loads(await send_to_orchestrator(websocket, myReq))
        print(json.dumps(job, indent=2))

        myReq = {
            "command": "p_updateJobStatus",
            "status": "Running",
            "hash": job["hash"],
            "task": job["task"],
            "token": token,
        }
        await send_to_orchestrator(websocket, myReq)

        time.sleep(1)
        myReq["status"] = "Done"
        await send_to_orchestrator(websocket, myReq)


# asyncio.run(hello())
asyncio.run(simulate_pilot())
