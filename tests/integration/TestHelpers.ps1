function Remove-TestDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (Test-Path $Path) {
        Remove-Item $Path -Recurse -Force
    }

    New-Item -ItemType Directory -Path $Path | Out-Null
}

function Start-DkvNode {
    param(
        [Parameter(Mandatory = $true)]
        [string]$NodeExe,

        [Parameter(Mandatory = $true)]
        [int]$Port,

        [Parameter(Mandatory = $true)]
        [string]$DataDir,

        [string[]]$Peers = @(),

        [switch]$BootstrapLeader,

        [switch]$NoBootstrapLeader
    )

    $arguments = @($Port.ToString(), '--server-only', '--data-dir', $DataDir)

    if ($BootstrapLeader) {
        $arguments += '--bootstrap-leader'
    }

    if ($NoBootstrapLeader) {
        $arguments += '--no-bootstrap-leader'
    }

    foreach ($peer in $Peers) {
        $arguments += '--peer'
        $arguments += $peer
    }

    $quotedArguments = $arguments | ForEach-Object {
        if ($_ -match '[\s"]') {
            '"' + ($_ -replace '"', '\"') + '"'
        } else {
            $_
        }
    }

    $stdoutPath = Join-Path $DataDir 'stdout.log'
    $stderrPath = Join-Path $DataDir 'stderr.log'
    New-Item -ItemType Directory -Path $DataDir -Force | Out-Null

    return Start-Process -FilePath $NodeExe `
        -ArgumentList ($quotedArguments -join ' ') `
        -PassThru `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath
}

function Stop-DkvNode {
    param(
        [Parameter(Mandatory = $true)]
        [System.Diagnostics.Process]$Process
    )

    if ($null -eq $Process) {
        return
    }

    if (-not $Process.HasExited) {
        Stop-Process -Id $Process.Id -Force
        $Process.WaitForExit()
    }
}

function Invoke-DkvCommand {
    param(
        [Parameter(Mandatory = $true)]
        [int]$Port,

        [Parameter(Mandatory = $true)]
        [string]$Command
    )

    $client = [System.Net.Sockets.TcpClient]::new()
    $client.Connect('127.0.0.1', $Port)

    try {
        $stream = $client.GetStream()
        $writer = [System.IO.StreamWriter]::new($stream)
        $reader = [System.IO.StreamReader]::new($stream)
        $writer.AutoFlush = $true
        $writer.WriteLine($Command)
        return $reader.ReadLine()
    }
    finally {
        $client.Close()
    }
}

function Wait-Until {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$Condition,

        [int]$TimeoutMs = 5000,

        [int]$IntervalMs = 125,

        [string]$FailureMessage = 'Timed out waiting for condition.'
    )

    $deadline = (Get-Date).AddMilliseconds($TimeoutMs)
    while ((Get-Date) -lt $deadline) {
        if (& $Condition) {
            return
        }

        Start-Sleep -Milliseconds $IntervalMs
    }

    throw $FailureMessage
}

function Parse-InfoLine {
    param(
        [Parameter(Mandatory = $true)]
        [string]$InfoLine
    )

    $result = @{}
    foreach ($token in $InfoLine.Split(' ')) {
        if ($token -match '=') {
            $parts = $token.Split('=', 2)
            $result[$parts[0]] = $parts[1]
        }
    }

    return $result
}

function Assert-Equal {
    param(
        [Parameter(Mandatory = $true)]$Actual,
        [Parameter(Mandatory = $true)]$Expected,
        [Parameter(Mandatory = $true)][string]$Message
    )

    if ($Actual -ne $Expected) {
        throw "$Message Expected '$Expected' but got '$Actual'."
    }
}

function Assert-True {
    param(
        [Parameter(Mandatory = $true)][bool]$Condition,
        [Parameter(Mandatory = $true)][string]$Message
    )

    if (-not $Condition) {
        throw $Message
    }
}