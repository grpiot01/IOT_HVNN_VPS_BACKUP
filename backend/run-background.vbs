Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "C:\backend"
WshShell.Run "C:\backend\.venv\Scripts\python.exe -m uvicorn main:app --host 0.0.0.0 --port 10000", 0, False
