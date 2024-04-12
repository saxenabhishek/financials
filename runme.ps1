Start-Process 'http://localhost:8000'
poetry run uvicorn src.__main__:app --reload --reload-dir "./src"
