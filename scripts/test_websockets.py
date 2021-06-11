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
        "executable": "./testScript.sh",
        "exe_args": [f"{nJobs}"],
        "status": "Done",
        "stdin": "",
        "stdout": "test.out",
        "stderr": "test.err",
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


async def hello():
    uri = "ws://localhost:9002"
    async with websockets.connect(uri) as websocket:
        myReq = {"command": "cleanTask", "task": "sometask"}

        print("Cleaning the task...")
        await send_to_orchestrator(websocket, myReq)

        for ijob in range(0, 3):
            newJob = getNextJob()

            myReq = {"command": "submitJob", "job": newJob}

            print("Sending a valid JSON job...")
            await send_to_orchestrator(websocket, myReq)

        # send invalid job
        # print("Sending an invalid message")
        # await websocket.send("aaaa:;jsfdbu348231@3327t8{)")
        # response = await websocket.recv()
        # print(f"Server replied: {response}")


asyncio.get_event_loop().run_until_complete(hello())
