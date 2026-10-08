import hashlib
import json
from pathlib import Path
import shutil
import subprocess
from tempfile import TemporaryDirectory
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "g5_behavior_preflight.ps1"
POWERSHELL = shutil.which("pwsh") or shutil.which("powershell")


@unittest.skipUnless(POWERSHELL, "PowerShell is required")
class G5BehaviorPreflightTests(unittest.TestCase):
    def _run(self, body: str) -> dict:
        command = f"$ErrorActionPreference='Stop'; . '{SCRIPT}' -FunctionsOnly; {body}"
        result = subprocess.run(
            [POWERSHELL, "-NoProfile", "-NonInteractive", "-Command", command],
            cwd=ROOT, text=True, capture_output=True, timeout=20,
        )
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        return json.loads(result.stdout)

    def test_source_hash_and_byte_count_fail_closed(self):
        with TemporaryDirectory() as directory:
            source = Path(directory) / "source.fixture"
            source.write_bytes(b"fixture only\n")
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            path = str(source).replace("'", "''")
            payload = self._run(f"""
function Rejected([scriptblock]$Action) {{
    try {{ & $Action | Out-Null; return $false }} catch {{ return $true }}
}}
$path = '{path}'
[ordered]@{{
    valid = -not (Rejected {{ Assert-G5SourceArtifact -Path $path -ExpectedBytes 13 -ExpectedSha256 '{digest}' }})
    bad_bytes = Rejected {{ Assert-G5SourceArtifact -Path $path -ExpectedBytes 12 -ExpectedSha256 '{digest}' }}
    bad_hash = Rejected {{ Assert-G5SourceArtifact -Path $path -ExpectedBytes 13 -ExpectedSha256 ('0' * 64) }}
}} | ConvertTo-Json -Compress
""")
            self.assertEqual({"valid": True, "bad_bytes": True, "bad_hash": True}, payload)

    def test_capacity_and_missing_pilot_stop_full_sample(self):
        payload = self._run(r"""
function Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$gib = [long]1GB
$pilot10 = [pscustomobject]@{ status='PASS'; stage=10000; d_delta_bytes=(2 * $gib) }
$pilot100 = [pscustomobject]@{ status='PASS'; stage=100000; d_delta_bytes=(3 * $gib) }
[ordered]@{
    c_low = Rejected { Assert-G5Capacity -Stage 10000 -CFreeBytes (4 * $gib) -DFreeBytes (30 * $gib) }
    d_low = Rejected { Assert-G5Capacity -Stage 10000 -CFreeBytes (6 * $gib) -DFreeBytes (19 * $gib) }
    full_no_pilot = Rejected { Assert-G5StageHistory -Stage 2199938 -Pilot10k $pilot10 -Pilot100k $null }
    full_too_small = Rejected {
        Assert-G5Capacity -Stage 2199938 -CFreeBytes (6 * $gib) -DFreeBytes (30 * $gib) -Pilot100kDeltaBytes (3 * $gib)
    }
    valid = -not (Rejected {
        Assert-G5Capacity -Stage 2199938 -CFreeBytes (6 * $gib) -DFreeBytes (110 * $gib) -Pilot100kDeltaBytes (3 * $gib)
    })
} | ConvertTo-Json -Compress
""")
        self.assertTrue(all(payload.values()), payload)

    def test_nonempty_target_and_reused_run_are_rejected(self):
        payload = self._run(r"""
function Rejected([scriptblock]$Action) {
    try { & $Action | Out-Null; return $false } catch { return $true }
}
$base = @{ RunId='g5-a10k-01'; RawTopicCount=0; CleanTopicCount=0; LateTopicCount=0; TableRowCount=0; CheckpointExists=$false; JobHistoryCount=0 }
$target = $base.Clone(); $target.TableRowCount = 1
$topic = $base.Clone(); $topic.RawTopicCount = 1
$job = $base.Clone(); $job.JobHistoryCount = 1
$checkpoint = $base.Clone(); $checkpoint.CheckpointExists = $true
[ordered]@{
    valid = -not (Rejected { Assert-G5FreshRun @base })
    target_rows = Rejected { Assert-G5FreshRun @target }
    topic_reused = Rejected { Assert-G5FreshRun @topic }
    job_reused = Rejected { Assert-G5FreshRun @job }
    checkpoint_reused = Rejected { Assert-G5FreshRun @checkpoint }
    unsafe_id = Rejected { Assert-G5FreshRun -RunId 'bad;id' -RawTopicCount 0 -CleanTopicCount 0 -LateTopicCount 0 -TableRowCount 0 -CheckpointExists $false -JobHistoryCount 0 }
} | ConvertTo-Json -Compress
""")
        self.assertTrue(all(payload.values()), payload)

    def test_script_never_contains_mutating_container_or_topic_commands(self):
        content = SCRIPT.read_text(encoding="utf-8")
        for forbidden in ("docker compose up", "docker run", "kafka-topics --create", "DROP TABLE", "TRUNCATE TABLE", "Remove-Item"):
            self.assertNotIn(forbidden, content)


if __name__ == "__main__":
    unittest.main()
