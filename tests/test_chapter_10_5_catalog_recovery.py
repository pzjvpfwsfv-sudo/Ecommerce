import json
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "restore_chapter_10_5_catalog.ps1"
TABLE_LOCATION = "s3a://warehouse/iceberg/analytics.db/user_behavior_detail"
UUID_ONE = "11111111-1111-4111-8111-111111111111"
UUID_TWO = "22222222-2222-4222-8222-222222222222"


class Chapter105CatalogRecoveryTest(unittest.TestCase):
    def _run_powershell(self, command):
        return subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )

    def _payload(self, command):
        result = self._run_powershell(command)
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        return json.loads(result.stdout.strip().splitlines()[-1])

    def _registration_race_payload(self, recheck_line, validation_statement):
        command = r'''
. (Resolve-Path "scripts/restore_chapter_10_5_catalog.ps1") -FunctionsOnly
$script:infoChecks = 0
$script:registerCalls = 0
$script:aggregateChecks = 0
function Invoke-Chapter105Compose {
    param([string[]]$Arguments, [string]$FailureMessage)
    $last = [string]$Arguments[-1]
    if ($last -match "information_schema") {
        $script:infoChecks++
        if ($script:infoChecks -eq 1) { return @("table_count", "0") }
        return @("table_count", "__RECHECK_LINE__")
    }
    if ($last -match "mc ls --recursive --json") {
        return @('{"key":"iceberg/analytics.db/user_behavior_detail/metadata/00012-22222222-2222-4222-8222-222222222222.metadata.json"}')
    }
    if ($last -match "mc cat") {
        return @('{"location":"s3a://warehouse/iceberg/analytics.db/user_behavior_detail","table-uuid":"22222222-2222-4222-8222-222222222222","current-snapshot-id":12}')
    }
    if ($last -match "register_table") {
        $script:registerCalls++
        throw "registration-secret-minioadmin123 response-body"
    }
    if ($last -match "event_count") {
        $script:aggregateChecks++
        __VALIDATION_STATEMENT__
    }
    throw "unexpected controlled command"
}
$status = ""
$failureMessage = ""
try { $status = Restore-Chapter105Catalog } catch { $failureMessage = $_.Exception.Message }
[ordered]@{
    status = $status
    failure = $failureMessage
    info_checks = $script:infoChecks
    register_calls = $script:registerCalls
    aggregate_checks = $script:aggregateChecks
} | ConvertTo-Json -Compress
'''
        command = command.replace("__RECHECK_LINE__", recheck_line)
        command = command.replace("__VALIDATION_STATEMENT__", validation_statement)
        return self._payload(command)

    def test_metadata_candidate_selection_rejects_empty_malformed_and_duplicate_highest(self):
        payload = self._payload(
            r'''
. (Resolve-Path "scripts/restore_chapter_10_5_catalog.ps1") -FunctionsOnly
$emptyRejected = $false
$malformedRejected = $false
$duplicateRejected = $false
try { Select-IcebergMetadataCandidate -Names @() | Out-Null } catch { $emptyRejected = $true }
try { Select-IcebergMetadataCandidate -Names @("00001-11111111-1111-4111-8111-111111111111.metadata.json", "notes.txt") | Out-Null } catch { $malformedRejected = $true }
try {
    Select-IcebergMetadataCandidate -Names @(
        "00002-11111111-1111-4111-8111-111111111111.metadata.json",
        "00002-22222222-2222-4222-8222-222222222222.metadata.json"
    ) | Out-Null
} catch { $duplicateRejected = $true }
[ordered]@{
    empty_rejected = $emptyRejected
    malformed_rejected = $malformedRejected
    duplicate_rejected = $duplicateRejected
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(
            {"empty_rejected": True, "malformed_rejected": True, "duplicate_rejected": True},
            payload,
        )

    def test_metadata_candidate_selection_returns_unique_highest_version(self):
        payload = self._payload(
            r'''
. (Resolve-Path "scripts/restore_chapter_10_5_catalog.ps1") -FunctionsOnly
Select-IcebergMetadataCandidate -Names @(
    "00001-11111111-1111-4111-8111-111111111111.metadata.json",
    "00012-22222222-2222-4222-8222-222222222222.metadata.json",
    "00011-11111111-1111-4111-8111-111111111111.metadata.json"
) | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(f"00012-{UUID_TWO}.metadata.json", payload)

    def test_metadata_validation_rejects_wrong_location_and_non_scalar_types_without_leaks(self):
        payload = self._payload(
            r'''
. (Resolve-Path "scripts/restore_chapter_10_5_catalog.ps1") -FunctionsOnly
$valid = '{"location":"s3a://warehouse/iceberg/analytics.db/user_behavior_detail","table-uuid":"11111111-1111-4111-8111-111111111111","current-snapshot-id":7}' | ConvertFrom-Json
Assert-IcebergMetadata -Metadata $valid | Out-Null
$wrongLocation = '{"location":"s3a://other/secret-minioadmin123","table-uuid":"11111111-1111-4111-8111-111111111111","current-snapshot-id":7}' | ConvertFrom-Json
$wrongType = '{"location":"s3a://warehouse/iceberg/analytics.db/user_behavior_detail","table-uuid":"11111111-1111-4111-8111-111111111111","current-snapshot-id":true}' | ConvertFrom-Json
$locationError = ""
$typeError = ""
try { Assert-IcebergMetadata -Metadata $wrongLocation | Out-Null } catch { $locationError = $_.Exception.Message }
try { Assert-IcebergMetadata -Metadata $wrongType | Out-Null } catch { $typeError = $_.Exception.Message }
[ordered]@{
    location_rejected = -not [string]::IsNullOrWhiteSpace($locationError)
    type_rejected = -not [string]::IsNullOrWhiteSpace($typeError)
    location_safe = $locationError -notmatch "secret-minioadmin123|minioadmin123"
    type_safe = $typeError -notmatch "secret-minioadmin123|minioadmin123"
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(
            {
                "location_rejected": True,
                "type_rejected": True,
                "location_safe": True,
                "type_safe": True,
            },
            payload,
        )

    def test_functions_only_import_does_not_call_docker_or_change_error_preference(self):
        payload = self._payload(
            r'''
$before = $ErrorActionPreference
$script:dockerCalls = 0
function docker { $script:dockerCalls++; throw "docker must not run during FunctionsOnly import" }
. (Resolve-Path "scripts/restore_chapter_10_5_catalog.ps1") -FunctionsOnly
[ordered]@{
    docker_calls = $script:dockerCalls
    error_preference_unchanged = $before -eq $ErrorActionPreference
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual({"docker_calls": 0, "error_preference_unchanged": True}, payload)

    def test_compose_wrapper_passes_fixed_prefix_and_caller_arguments_as_exact_argv(self):
        payload = self._payload(
            r'''
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
. (Resolve-Path "scripts/restore_chapter_10_5_catalog.ps1") -FunctionsOnly
$script:capturedArguments = @()
function docker {
    $script:capturedArguments = @($args)
    $global:LASTEXITCODE = 0
    return "native-result"
}
$output = @(Invoke-Chapter105Compose -Arguments @(
    "exec", "-T", "trino", "trino", "--execute", "SELECT 1"
) -FailureMessage "Catalog compose command failed.")
[ordered]@{
    arguments = $script:capturedArguments
    output = $output
} | ConvertTo-Json -Compress -Depth 4
'''
        )
        self.assertEqual(
            [
                "compose",
                "--env-file",
                str(ROOT / "infra" / ".env.example"),
                "-f",
                str(ROOT / "infra" / "docker-compose.yml"),
                "--profile",
                "lakehouse",
                "exec",
                "-T",
                "trino",
                "trino",
                "--execute",
                "SELECT 1",
            ],
            payload["arguments"],
        )
        self.assertEqual(["native-result"], payload["output"])

    def test_compose_wrapper_scrubs_nonzero_native_output_and_credentials(self):
        secret = "native-secret-minioadmin123 response-body"
        result = self._run_powershell(
            r'''
. (Resolve-Path "scripts/restore_chapter_10_5_catalog.ps1") -FunctionsOnly
function docker {
    $global:LASTEXITCODE = 23
    return "native-secret-minioadmin123 response-body"
}
$capturedOutput = @()
$failureMessage = ""
try {
    $capturedOutput = @(Invoke-Chapter105Compose -Arguments @("exec", "-T", "trino") `
        -FailureMessage "Catalog compose command failed.")
} catch {
    $failureMessage = $_.Exception.Message
}
[ordered]@{
    failure = $failureMessage
    emitted = $capturedOutput
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        self.assertNotIn(secret, result.stdout + result.stderr)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual("Catalog compose command failed.", payload["failure"])
        self.assertEqual([], payload["emitted"])

    def test_existing_readable_table_is_a_no_op(self):
        payload = self._payload(
            r'''
. (Resolve-Path "scripts/restore_chapter_10_5_catalog.ps1") -FunctionsOnly
$script:commands = @()
function Invoke-Chapter105Compose {
    param([string[]]$Arguments, [string]$FailureMessage)
    $script:commands += ,@($Arguments)
    $sql = [string]$Arguments[-1]
    if ($sql -match "information_schema") { return @("table_count", "1") }
    if ($sql -match "SELECT 1 FROM lakehouse\.analytics\.user_behavior_detail LIMIT 1") { return @("_col0") }
    throw "unexpected controlled command"
}
$status = Restore-Chapter105Catalog
[ordered]@{
    status = $status
    command_count = $script:commands.Count
    used_mc = (@($script:commands | Where-Object { $_ -contains "run" })).Count -gt 0
    used_register = (@($script:commands | Where-Object { ([string]$_[-1]) -match "register_table" })).Count -gt 0
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(
            {"status": "already_registered", "command_count": 2, "used_mc": False, "used_register": False},
            payload,
        )

    def test_absent_table_restores_only_the_fixed_identity_with_validated_basename(self):
        payload = self._payload(
            r'''
. (Resolve-Path "scripts/restore_chapter_10_5_catalog.ps1") -FunctionsOnly
$script:commands = @()
function Invoke-Chapter105Compose {
    param([string[]]$Arguments, [string]$FailureMessage)
    $script:commands += ,@($Arguments)
    $last = [string]$Arguments[-1]
    if ($last -match "information_schema") { return @("table_count", "0") }
    if ($last -match "mc ls --recursive --json") {
        return @('{"key":"iceberg/analytics.db/user_behavior_detail/metadata/00012-22222222-2222-4222-8222-222222222222.metadata.json"}')
    }
    if ($last -match "mc cat") {
        return @('{"location":"s3a://warehouse/iceberg/analytics.db/user_behavior_detail","table-uuid":"22222222-2222-4222-8222-222222222222","current-snapshot-id":12}')
    }
    if ($last -match "register_table") { return @("registered") }
    if ($last -match "event_count") { return @("event_count,max_event_time", "0,") }
    throw "unexpected controlled command"
}
$status = Restore-Chapter105Catalog
$registerSql = @($script:commands | ForEach-Object { [string]$_[-1] } | Where-Object { $_ -match "register_table" })[0]
[ordered]@{
    status = $status
    command_count = $script:commands.Count
    fixed_schema = $registerSql -match "schema_name => 'analytics'"
    fixed_table = $registerSql -match "table_name => 'user_behavior_detail'"
    fixed_location = $registerSql -match "table_location => 's3a://warehouse/iceberg/analytics\.db/user_behavior_detail'"
    exact_metadata_name = $registerSql -match "metadata_file_name => '00012-22222222-2222-4222-8222-222222222222\.metadata\.json'"
    no_untrusted_identifier = $registerSql -notmatch "other_catalog|other_schema|other_table"
    credentials_not_passed = (@($script:commands | ForEach-Object { $_ -join " " } | Where-Object { $_ -match "minioadmin123" })).Count -eq 0
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual(
            {
                "status": "restored",
                "command_count": 5,
                "fixed_schema": True,
                "fixed_table": True,
                "fixed_location": True,
                "exact_metadata_name": True,
                "no_untrusted_identifier": True,
                "credentials_not_passed": True,
            },
            payload,
        )

    def test_registration_race_rechecks_once_and_accepts_only_verified_fixed_table(self):
        payload = self._registration_race_payload(
            "1", 'return @("event_count,max_event_time", "0,")'
        )

        self.assertEqual("registered_by_another_actor", payload["status"])
        self.assertEqual("", payload["failure"])
        self.assertEqual(2, payload["info_checks"])
        self.assertEqual(1, payload["register_calls"])
        self.assertEqual(1, payload["aggregate_checks"])

    def test_registration_race_with_table_still_absent_fails_scrubbed_without_retry(self):
        payload = self._registration_race_payload(
            "0", 'return @("event_count,max_event_time", "0,")'
        )

        self.assertEqual("", payload["status"])
        self.assertEqual("Fixed Iceberg catalog registration failed.", payload["failure"])
        self.assertNotIn("registration-secret", payload["failure"])
        self.assertEqual(2, payload["info_checks"])
        self.assertEqual(1, payload["register_calls"])
        self.assertEqual(0, payload["aggregate_checks"])

    def test_registration_race_with_malformed_recheck_fails_scrubbed_without_retry(self):
        payload = self._registration_race_payload(
            "not-a-count", 'return @("event_count,max_event_time", "0,")'
        )

        self.assertEqual("", payload["status"])
        self.assertEqual("Fixed Iceberg catalog registration failed.", payload["failure"])
        self.assertNotIn("not-a-count", payload["failure"])
        self.assertEqual(2, payload["info_checks"])
        self.assertEqual(1, payload["register_calls"])
        self.assertEqual(0, payload["aggregate_checks"])

    def test_registration_race_with_failed_verification_fails_scrubbed_without_retry(self):
        payload = self._registration_race_payload(
            "1", 'throw "verification-secret response-body"'
        )

        self.assertEqual("", payload["status"])
        self.assertEqual("Fixed Iceberg catalog registration failed.", payload["failure"])
        self.assertNotIn("verification-secret", payload["failure"])
        self.assertEqual(2, payload["info_checks"])
        self.assertEqual(1, payload["register_calls"])
        self.assertEqual(1, payload["aggregate_checks"])

    def test_malformed_information_schema_output_fails_closed_before_metadata_access(self):
        payload = self._payload(
            r'''
. (Resolve-Path "scripts/restore_chapter_10_5_catalog.ps1") -FunctionsOnly
$script:commands = @()
function Invoke-Chapter105Compose {
    param([string[]]$Arguments, [string]$FailureMessage)
    $script:commands += ,@($Arguments)
    return @("table_count", "not-a-count")
}
$failureMessage = ""
try { Restore-Chapter105Catalog | Out-Null } catch { $failureMessage = $_.Exception.Message }
[ordered]@{
    rejected = -not [string]::IsNullOrWhiteSpace($failureMessage)
    safe = $failureMessage -notmatch "not-a-count|minioadmin123"
    metadata_not_accessed = (@($script:commands | Where-Object { $_ -contains "run" })).Count -eq 0
} | ConvertTo-Json -Compress
'''
        )
        self.assertEqual({"rejected": True, "safe": True, "metadata_not_accessed": True}, payload)


if __name__ == "__main__":
    unittest.main()
