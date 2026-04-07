$currentDir = Get-Location
Write-Host "Starting BZA Keyword Expander on Port 7888..."
Write-Host "Access locally at: http://localhost:7888"
Write-Host "To share with others, find your IP address using 'ipconfig'"
Write-Host "Example: http://192.168.1.x:7888"
Write-Host "Ensure Windows Firewall allows port 7888."

# Activate virtual environment if it exists (adjust path as needed)
if (Test-Path "..\.venv\Scripts\Activate.ps1") {
    & "..\.venv\Scripts\Activate.ps1"
}

# Ensure we are in the script's directory
Set-Location $PSScriptRoot

# Run the server
uvicorn app:app --host 0.0.0.0 --port 7888 --reload
