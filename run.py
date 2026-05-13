# Entry point — run with:
#     python run.py
# or:
#     C:/Users/David/miniconda3/Scripts/uvicorn.exe app.main:app --reload
import uvicorn

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
