[CmdletBinding()]
param(
    [ValidateSet('simulation', 'trading', 'both', 'shadow', 'demo')]
    [string]$Mode = 'simulation',
    [switch]$Build,
    [switch]$Foreground,
    [switch]$DryRun,
    [string]$ProjectName,
    [string]$PythonPath
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$taskArguments = @((Join-Path $PSScriptRoot 'start.py'), '--mode', $Mode)
if ($Build) { $taskArguments += '--build' }
if ($Foreground) { $taskArguments += '--foreground' }
if ($DryRun) { $taskArguments += '--dry-run' }
if ($ProjectName) { $taskArguments += @('--project-name', $ProjectName) }
if (-not $PythonPath) {
    $taskPython = Get-Command python3, python, py -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $taskPython) { throw 'Python 3 is required. Pass -PythonPath with its absolute path.' }
    $PythonPath = $taskPython.Source
    if ($taskPython.Name -eq 'py.exe' -or $taskPython.Name -eq 'py') { $taskArguments = @('-3') + $taskArguments }
}
& $PythonPath @taskArguments
exit $LASTEXITCODE
