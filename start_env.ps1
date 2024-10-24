# Set the Python home directory and add it to the PATH
$env:PYTHON_HOME = "C:\Users\izzaz\AppData\Local\Programs\Python\Python311"
$env:PATH = "$env:PYTHON_HOME;$env:PYTHON_HOME\Scripts;$env:PATH"

# Activate the virtual environment
.venv\Scripts\Activate.ps1

# Optional: Print environment variables to confirm they're set
Write-Output "PYTHON_HOME is set to: $env:PYTHON_HOME"
Write-Output "GOOGLE_APPLICATION_CREDENTIALS is set to: $env:GOOGLE_APPLICATION_CREDENTIALS"
Write-Output "Virtual environment activated. You can now run your Python scripts."