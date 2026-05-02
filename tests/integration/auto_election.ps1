param(
    [Parameter(Mandatory = $true)]
    [string]$NodeExe,

    [Parameter(Mandatory = $true)]
    [string]$RuntimeRoot
)

. $PSScriptRoot/TestHelpers.ps1

$nodeA = $null
$nodeB = $null

try {
    Remove-TestDirectory -Path $RuntimeRoot
    $dataA = Join-Path $RuntimeRoot 'node-a'
    $dataB = Join-Path $RuntimeRoot 'node-b'

    $nodeA = Start-DkvNode -NodeExe $NodeExe -Port 7400 -DataDir $dataA -Peers @('127.0.0.1:7401') -NoBootstrapLeader
    $nodeB = Start-DkvNode -NodeExe $NodeExe -Port 7401 -DataDir $dataB -Peers @('127.0.0.1:7400') -NoBootstrapLeader

    $leaderPort = 0
    Wait-Until -TimeoutMs 7000 -FailureMessage 'Cluster did not elect a leader in time.' -Condition {
        try {
            $infoA = Parse-InfoLine (Invoke-DkvCommand -Port 7400 -Command 'INFO')
            $infoB = Parse-InfoLine (Invoke-DkvCommand -Port 7401 -Command 'INFO')

            if ($infoA['role'] -eq 'leader' -and $infoB['role'] -eq 'follower') {
                $script:leaderPort = 7400
                return $true
            }

            if ($infoB['role'] -eq 'leader' -and $infoA['role'] -eq 'follower') {
                $script:leaderPort = 7401
                return $true
            }

            return $false
        } catch {
            return $false
        }
    }

    Assert-True -Condition ($leaderPort -ne 0) -Message 'Exactly one node should be elected leader.'

    $followerPort = if ($leaderPort -eq 7400) { 7401 } else { 7400 }
    $putResult = Invoke-DkvCommand -Port $leaderPort -Command 'PUT session:auto elected'
    Assert-Equal -Actual $putResult -Expected 'OK' -Message 'Write through elected leader should succeed.'

    Wait-Until -TimeoutMs 5000 -FailureMessage 'Follower did not apply elected leader write in time.' -Condition {
        try {
            return (Invoke-DkvCommand -Port $followerPort -Command 'GET session:auto') -eq 'VALUE elected'
        } catch {
            return $false
        }
    }

    $leaderInfo = Parse-InfoLine (Invoke-DkvCommand -Port $leaderPort -Command 'INFO')
    $followerInfo = Parse-InfoLine (Invoke-DkvCommand -Port $followerPort -Command 'INFO')

    Assert-Equal -Actual $leaderInfo['role'] -Expected 'leader' -Message 'One node must remain leader after election.'
    Assert-Equal -Actual $followerInfo['role'] -Expected 'follower' -Message 'The other node must remain follower after election.'
    Assert-Equal -Actual $followerInfo['leader'] -Expected "127.0.0.1:$leaderPort" -Message 'Follower should report the elected leader.'
}
finally {
    if ($nodeA) {
        Stop-DkvNode -Process $nodeA
    }

    if ($nodeB) {
        Stop-DkvNode -Process $nodeB
    }
}