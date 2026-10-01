[CmdletBinding()]
param(
    [string]$EnvPath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.env')
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$taskEnvPath = [System.IO.Path]::GetFullPath($EnvPath)
if (Test-Path -LiteralPath $taskEnvPath) {
    Write-Host '.env already exists. Its contents were left unchanged.'
    exit 0
}

$taskBytes = New-Object byte[] 64
$taskRng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$taskStream = $null
$taskCreated = $false
$taskWritten = $false
try {
    $taskRng.GetBytes($taskBytes)
    $taskRandom = [System.BitConverter]::ToString($taskBytes).Replace('-', '').ToLowerInvariant()
    $taskToken = $taskRandom.Substring(0, 64)
    $taskConsoleToken = $taskRandom.Substring(64, 64)
    $taskStream = [System.IO.File]::Open($taskEnvPath, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    $taskCreated = $true
    # Restrict the empty file before writing a secret. CreateNew also prevents overwriting a racing invocation.
    if ($env:OS -eq 'Windows_NT') {
        $taskOwner = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
        $taskAcl = New-Object System.Security.AccessControl.FileSecurity
        $taskAcl.SetAccessRuleProtection($true, $false)
        $taskAcl.SetOwner($taskOwner)
        foreach ($taskSid in @($taskOwner, [System.Security.Principal.SecurityIdentifier]::new('S-1-5-18'), [System.Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))) {
            $taskRule = [System.Security.AccessControl.FileSystemAccessRule]::new($taskSid, [System.Security.AccessControl.FileSystemRights]::FullControl, [System.Security.AccessControl.AccessControlType]::Allow)
            $taskAcl.AddAccessRule($taskRule)
        }
        Set-Acl -LiteralPath $taskEnvPath -AclObject $taskAcl
    } else {
        & chmod 600 -- $taskEnvPath
        if ($LASTEXITCODE -ne 0) { throw 'Could not restrict .env permissions.' }
    }
    $taskText = "MM2_ENGINE_API_TOKEN=$taskToken`nMM2_CONSOLE_TOKEN=$taskConsoleToken`nMM2_TELEGRAM_BOT_TOKEN=`nMM2_TELEGRAM_OWNER_ID=`nMM2_MINIAPP_URL=`nMM2_ENGINE_CONFIG=./config/demo.toml`nMM2_ALLOWED_ORIGINS=http://127.0.0.1:8080,http://localhost:8080`n"
    $taskContent = [System.Text.UTF8Encoding]::new($false).GetBytes($taskText)
    $taskStream.Write($taskContent, 0, $taskContent.Length)
    $taskStream.Flush($true)
    $taskWritten = $true
} finally {
    if ($null -ne $taskStream) { $taskStream.Dispose() }
    $taskRng.Dispose()
    [System.Array]::Clear($taskBytes, 0, $taskBytes.Length)
    if ($taskCreated -and -not $taskWritten) { [System.IO.File]::Delete($taskEnvPath) }
}
Write-Host 'Created private .env with a random API token. No value was printed.'
