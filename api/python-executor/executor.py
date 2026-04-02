from fastapi import FastAPI
import pandas as pd
import requests
import tempfile

app = FastAPI()

@app.get("/")
def home():
    return {"status": "Python executor running"}

@app.post("/execute")
async def execute_code(payload: dict):
    code = payload.get("code")
    file_url = payload.get("file_url")

    try:
        # Download file from URL
        response = requests.get(file_url)
        temp = tempfile.NamedTemporaryFile(delete=False)
        temp.write(response.content)
        temp.close()

        # Read file
        if file_url.endswith(".csv"):
            df = pd.read_csv(temp.name)
        else:
            df = pd.read_excel(temp.name)

        local_vars = {"df": df}

        # SAFE EXECUTION
        exec(code, {"__builtins__": {}}, local_vars)

        result = local_vars.get("result", None)

        return {
            "success": True,
            "result": str(result)
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
