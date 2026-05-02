param(
    [Parameter(Mandatory = $true)]
    [string]$NodeExe,

    [Parameter(Mandatory = $true)]
    [string]$RuntimeRoot
)

. $PSScriptRoot/TestHelpers.ps1

$node = $null

try {
    Remove-TestDirectory -Path $RuntimeRoot
    $dataDir = Join-Path $RuntimeRoot 'node'

    $node = Start-DkvNode -NodeExe $NodeExe -Port 7500 -DataDir $dataDir -BootstrapLeader

    Wait-Until -TimeoutMs 5000 -FailureMessage 'Single node leader did not start in time.' -Condition {
        try {
            $info = Parse-InfoLine (Invoke-DkvCommand -Port 7500 -Command 'INFO')
            return $info['role'] -eq 'leader'
        } catch {
            return $false
        }
    }

    Assert-Equal -Actual (Invoke-DkvCommand -Port 7500 -Command 'PUT profile:1 jane') -Expected 'OK' -Message 'Write before restart should succeed.'
    Assert-Equal -Actual (Invoke-DkvCommand -Port 7500 -Command 'RAFT REQUEST_VOTE 3 candidate-z 1 1') -Expected 'RAFT REQUEST_VOTE_RESPONSE 3 1' -Message 'Vote request should advance persisted term and vote.'

    $beforeRestart = Parse-InfoLine (Invoke-DkvCommand -Port 7500 -Command 'INFO')
    Assert-Equal -Actual $beforeRestart['commit_index'] -Expected '1' -Message 'Node should commit the written entry before restart.'

    Stop-DkvNode -Process $node
    $node = $null

    $stateFile = Join-Path $dataDir 'raft.state'
    $logFile = Join-Path $dataDir 'raft.log'
    Assert-True -Condition (Test-Path $stateFile) -Message 'Persistent state file should exist after shutdown.'
    Assert-True -Condition (Test-Path $logFile) -Message 'Persistent log file should exist after shutdown.'

    $node = Start-DkvNode -NodeExe $NodeExe -Port 7500 -DataDir $dataDir -NoBootstrapLeader

    Wait-Until -TimeoutMs 5000 -FailureMessage 'Restarted node did not become reachable in time.' -Condition {
        try {
            Invoke-DkvCommand -Port 7500 -Command 'PING' | Out-Null
            return $true
        } catch {
            return $false
        }
    }

    $afterRestart = Parse-InfoLine (Invoke-DkvCommand -Port 7500 -Command 'INFO')
    Assert-True -Condition ([int]$afterRestart['term'] -ge 3) -Message 'Restarted node should restore or advance persisted term.'
    Assert-Equal -Actual $afterRestart['commit_index'] -Expected '1' -Message 'Restarted node should preserve committed index.'
    Assert-Equal -Actual (Invoke-DkvCommand -Port 7500 -Command 'GET profile:1') -Expected 'VALUE jane' -Message 'Restarted node should recover committed key-value state.'
}
finally {
    if ($node) {
        Stop-DkvNode -Process $node
    }
}