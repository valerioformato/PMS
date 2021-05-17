import asyncio
import json
import websockets

nJobs = 0


def getNextJob():
    global nJobs
    nJobs += 1
    return json.dumps({
        "task": "sometask",
        "subtask": "somesubtask",
        "dataset": "some_dataset",
        "user": "vformato",
        "executable": "./testScript.sh",
        "exe_args": [],
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
    })


async def hello():
    uri = "ws://localhost:9002"
    async with websockets.connect(uri) as websocket:

        for ijob in range(0, 3):
            newJob = getNextJob()

            print("Sending a valid JSON job...")
            await websocket.send(newJob)

            response = await websocket.recv()
            print(f"Server replied: {response}")

        # send invalid job
        print("Sending an invalid message")
        await websocket.send("aaaa:;jsfdbu348231@3327t8{)")
        response = await websocket.recv()
        print(f"Server replied: {response}")


asyncio.get_event_loop().run_until_complete(hello())
