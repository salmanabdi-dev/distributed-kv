$ErrorActionPreference = "Stop"

function Invoke-NativeChecked {
    param(
        [Parameter(Mandatory = $true)] [string] $Label,
        [Parameter(Mandatory = $true)] [scriptblock] $Command
    )
    Write-Host "==> $Label"
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Label failed with exit code $LASTEXITCODE"
    }
}

Write-Host "==> Generating protobuf code"
& "$PSScriptRoot/generate.ps1"
if ($LASTEXITCODE -ne 0) { throw "protobuf generation failed with exit code $LASTEXITCODE" }

Invoke-NativeChecked "Resolving Go modules" { go mod tidy }

Write-Host "==> Formatting"
$goFiles = Get-ChildItem -Recurse -Filter *.go | ForEach-Object { $_.FullName }
if ($goFiles.Count -gt 0) {
    gofmt -w $goFiles
    if ($LASTEXITCODE -ne 0) { throw "gofmt failed with exit code $LASTEXITCODE" }
}

Invoke-NativeChecked "Vet" { go vet ./... }
Invoke-NativeChecked "Tests" { go test ./... }

Write-Host "==> Race detector"
go test -race ./...
if ($LASTEXITCODE -ne 0) {
    throw "Race detector failed. If the output reports 'DATA RACE', fix the race before committing. If Go reports that -race is unsupported because a C toolchain is missing, install MinGW-w64 and rerun."
}

Write-Host "All checks passed: format, vet, tests, and race detector."
