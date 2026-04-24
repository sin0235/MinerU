param(
  [string]$PythonExe = "",
  [string]$VenvPath = ".venv-mineru"
)

$ErrorActionPreference = "Stop"

if (-not $PythonExe) {
  $candidate = & py -3.12 -c "import sys; print(sys.executable)" 2>$null
  if (-not $candidate) {
    throw "Khong tim thay Python 3.12 qua py launcher. Hay cai Python 3.12 hoac truyen -PythonExe <path>."
  }
  $PythonExe = $candidate.Trim()
}

& $PythonExe -c "import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 12) else 1)"
if ($LASTEXITCODE -ne 0) {
  throw "PythonExe phai la Python 3.12 tren Windows de tranh loi dependency cua MinerU."
}

& $PythonExe -m venv $VenvPath
$venvPython = Join-Path $VenvPath "Scripts\python.exe"
& $venvPython -m pip install --upgrade pip
& $venvPython -m pip install uv
& $venvPython -m uv pip install -U "mineru[all]"
& $venvPython -m uv pip install -U mineru_vl_utils
$downloadExe = Join-Path (Split-Path $venvPython) "mineru-models-download.exe"
if (Test-Path $downloadExe) {
  & $downloadExe -s huggingface -m all
} else {
  Write-Warning "Khong tim thay mineru-models-download.exe, bo qua buoc tai model."
}

Write-Host ""
Write-Host "MinerU env da san sang."
Write-Host "Dat bien moi truong truoc khi chay webapp:"
Write-Host "`$env:MINERU_PYTHON_EXE = `"$((Resolve-Path $venvPython).Path)`""
Write-Host "`$env:MINERU_MODEL_SOURCE = `"huggingface`""
Write-Host "`$env:MINERU_VL_MODEL_NAME = `"opendatalab/MinerU2.5-Pro-2604-1.2B`""
