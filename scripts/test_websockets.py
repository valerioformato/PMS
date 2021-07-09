import asyncio
import json
import websockets

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


async def hello():
    uri = "ws://localhost:9002"

    oldToken = "94964336-dda0-47fc-9d83-ba2e4675fd74"

    async with websockets.connect(uri) as websocket:
        print("Cleaning the task...")
        myReq = {"command": "cleanTask", "task": "sometask", "token": oldToken}
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

        # send invalid job
        # print("Sending an invalid message")
        # await websocket.send("aaaa:;jsfdbu348231@3327t8{)")
        # response = await websocket.recv()
        # print(f"Server replied: {response}")


asyncio.get_event_loop().run_until_complete(hello())
