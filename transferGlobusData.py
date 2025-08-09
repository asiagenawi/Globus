import subprocess
import json
import os
import time

searchUuid = "2707914f-bdeb-49ee-a4b8-54e645b3d885"
searchQuery = "asiagenawi-data"
sourceUuid = "0f2a4dce-dc3d-468a-86e7-d057c70aacba"
destUuid = "4c094860-5a65-4d31-b640-600ab54edf61"
batchSize = 10

def search():
    cmd = ["globus", "search", "query", "-q", searchQuery, searchUuid, "--format", "JSON"]
    with open("output.json", "w") as f:
        subprocess.run(cmd, stdout=f, check=True)

def extractSubdirs(data):
    subdirs = []
    for result in data.get("gmeta", []):
        for entry in result.get("entries", []):
            content = entry.get("content", {})
            name = content.get("name", "")
            path = content.get("globus", {}).get("transfer", {}).get("path", "")
            if name and path:
                subdirs.append((path, name))
    return subdirs

def transferEgg(sourceUuid, sourcePath, subdir, destUuid):
    fullSourceDir = os.path.join(sourcePath, subdir)
    cmd = ["globus", "ls", f"{sourceUuid}:{fullSourceDir}"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    files = result.stdout.strip().splitlines()

    taskIds = []
    for file in files:
        if file.endswith(".egg"):
            sourceFilePath = os.path.join(fullSourceDir, file)
            destDirPath = os.path.join("/", subdir)
            destFilePath = os.path.join(destDirPath, file)
            transferCmd = [
                "globus", "transfer",
                f"{sourceUuid}:{sourceFilePath}",
                f"{destUuid}:{destFilePath}"
            ]
            result = subprocess.run(transferCmd, capture_output=True, text=True, check=True)
            for line in result.stdout.strip().splitlines():
                if "Task ID:" in line:
                    taskId = line.split()[-1]
                    taskIds.append(taskId)
    return taskIds, fullSourceDir

def main():
    search()
    with open("output.json", "r") as f:
        data = json.load(f)

    subdirs = extractSubdirs(data)
    total = len(subdirs)
    i = 0

    while i < total:
        batch = subdirs[i:i + batchSize]
        for sourcePath, subdir in batch:
            parts = sourcePath.strip("/").split("/")
            finalSourcePath = "/" + "/".join(parts[1:])

            taskIds, fullSourceDir = transferEgg(sourceUuid, finalSourcePath, subdir, destUuid)

            allSucceeded = True
            for taskId in taskIds:
                result = subprocess.run(["globus", "task", "wait", taskId])
                if result.returncode != 0:
                    allSucceeded = False
                    break

        i += batchSize
        input()

if __name__ == "__main__":
    main()

