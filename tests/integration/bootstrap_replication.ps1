param(
    [Parameter(Mandatory = $true)]
    [string]$NodeExe,

    [Parameter(Mandatory = $true)]
    [string]$RuntimeRoot
)

. $PSScriptRoot/TestHelpers.ps1

$leader = $null
$follower = $null

try {
    Remove-TestDirectory -Path $RuntimeRoot
    $leaderData = Join-Path $RuntimeRoot 'leader'
    $followerData = Join-Path $RuntimeRoot 'follower'

    $follower = Start-DkvNode -NodeExe $NodeExe -Port 7301 -DataDir $followerData -Peers @('127.0.0.1:7300') -NoBootstrapLeader
    $leader = Start-DkvNode -NodeExe $NodeExe -Port 7300 -DataDir $leaderData -Peers @('127.0.0.1:7301') -BootstrapLeader

    Wait-Until -TimeoutMs 5000 -FailureMessage 'Leader did not advertise leader role in time.' -Condition {
        try {
            $info = Parse-InfoLine (Invoke-DkvCommand -Port 7300 -Command 'INFO')
            return $info['role'] -eq 'leader'
        } catch {
            return $false
        }
    }

    $putResult = Invoke-DkvCommand -Port 7300 -Command 'PUT account:42 active'
    Assert-Equal -Actual $putResult -Expected 'OK' -Message 'Leader write should succeed.'

    Wait-Until -TimeoutMs 5000 -FailureMessage 'Follower did not apply replicated value in time.' -Condition {
        try {
            return (Invoke-DkvCommand -Port 7301 -Command 'GET account:42') -eq 'VALUE active'
        } catch {
            return $false
        }
    }

    $followerRedirect = Invoke-DkvCommand -Port 7301 -Command 'PUT account:43 standby'
    Assert-Equal -Actual $followerRedirect -Expected 'REDIRECT 127.0.0.1:7300' -Message 'Follower should redirect client writes to leader.'

    $followerInfo = Parse-InfoLine (Invoke-DkvCommand -Port 7301 -Command 'INFO')
    Assert-Equal -Actual $followerInfo['leader'] -Expected '127.0.0.1:7300' -Message 'Follower should report the configured leader.'
    Assert-Equal -Actual $followerInfo['commit_index'] -Expected '1' -Message 'Follower should commit the replicated write.'
}
finally {
    if ($leader) {
        Stop-DkvNode -Process $leader
    }

    if ($follower) {
        Stop-DkvNode -Process $follower
    }
}