from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT / "infra" / ".env.example"
COMPOSE_FILE = ROOT / "infra" / "docker-compose.yml"
HADOOP_CORE_SITE = ROOT / "infra" / "compose" / "flink" / "conf" / "core-site.xml"
HADOOP_HDFS_SITE = ROOT / "infra" / "compose" / "flink" / "conf" / "hdfs-site.xml"
MINIO_README = ROOT / "infra" / "compose" / "minio" / "README.md"
ICEBERG_CATALOG_SQL = ROOT / "jobs" / "sql" / "06_create_iceberg_catalog.sql"
ICEBERG_SINK_SQL = ROOT / "jobs" / "sql" / "07_sink_user_behavior_to_iceberg.sql"
CHAPTER5_RUNNER = ROOT / "scripts" / "run_chapter_5_iceberg_pipeline.ps1"
README_FILE = ROOT / "README.md"
JOBS_README = ROOT / "jobs" / "README.md"


class Chapter5ArtifactsTest(unittest.TestCase):
    def test_env_file_includes_minio_and_iceberg_variables(self):
        text = ENV_FILE.read_text(encoding="utf-8")

        self.assertIn("MINIO_ROOT_USER=", text)
        self.assertIn("MINIO_ROOT_PASSWORD=", text)
        self.assertIn("MINIO_BUCKET=", text)
        self.assertIn("ICEBERG_CATALOG_NAME=", text)
        self.assertIn("ICEBERG_WAREHOUSE=", text)

    def test_compose_defines_minio_and_iceberg_runtime(self):
        text = COMPOSE_FILE.read_text(encoding="utf-8")

        self.assertIn("minio:", text)
        self.assertIn("minio-init:", text)
        self.assertIn("hive-metastore:", text)
        self.assertIn('profiles: ["lakehouse"]', text)
        self.assertIn("minio/minio", text)
        self.assertIn("minio/mc", text)
        self.assertIn("9083", text)
        self.assertIn("iceberg-flink-runtime-1.19-1.6.1.jar", text)
        self.assertIn("iceberg-aws-bundle-1.6.1.jar", text)
        self.assertIn("hadoop-client-api-3.3.6.jar", text)
        self.assertIn("hadoop-client-runtime-3.3.6.jar", text)
        self.assertIn("hadoop-aws-3.3.6.jar", text)
        self.assertIn("aws-java-sdk-bundle-1.12.262.jar", text)
        self.assertIn("HADOOP_CONF_DIR: /opt/hadoop-conf", text)
        self.assertIn("./compose/flink/conf:/opt/hadoop-conf:ro", text)
        self.assertIn("AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER}", text)
        self.assertIn("AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD}", text)
        self.assertIn("AWS_REGION: us-east-1", text)
        self.assertIn("AWS_EC2_METADATA_DISABLED: \"true\"", text)

    def test_hadoop_conf_supplies_minio_s3a_settings(self):
        core_text = HADOOP_CORE_SITE.read_text(encoding="utf-8")
        hdfs_text = HADOOP_HDFS_SITE.read_text(encoding="utf-8")

        self.assertIn("<name>fs.s3a.impl</name>", core_text)
        self.assertIn("<value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>", core_text)
        self.assertIn("<name>fs.s3a.endpoint</name>", core_text)
        self.assertIn("<value>minio:9000</value>", core_text)
        self.assertIn("<name>fs.s3a.access.key</name>", core_text)
        self.assertIn("<value>minioadmin</value>", core_text)
        self.assertIn("<name>fs.s3a.secret.key</name>", core_text)
        self.assertIn("<value>minioadmin123</value>", core_text)
        self.assertIn("<name>fs.s3a.path.style.access</name>", core_text)
        self.assertIn("<value>true</value>", core_text)
        self.assertIn("<configuration>", hdfs_text)

    def test_iceberg_sql_defines_catalog_and_sink(self):
        catalog_text = ICEBERG_CATALOG_SQL.read_text(encoding="utf-8")
        sink_text = ICEBERG_SINK_SQL.read_text(encoding="utf-8")

        self.assertIn("CREATE CATALOG lakehouse", catalog_text)
        self.assertIn("'type' = 'iceberg'", catalog_text)
        self.assertIn("'catalog-type' = 'hive'", catalog_text)
        self.assertIn("'uri' = 'thrift://hive-metastore:9083'", catalog_text)
        self.assertIn("'warehouse' = 's3a://warehouse/iceberg'", catalog_text)
        self.assertNotIn("'catalog-type' = 'hadoop'", catalog_text)
        self.assertNotIn("'fs.s3a.impl' = 'org.apache.hadoop.fs.s3a.S3AFileSystem'", catalog_text)
        self.assertNotIn("'fs.s3a.endpoint' = 'minio:9000'", catalog_text)
        self.assertNotIn("'fs.s3a.access.key' = 'minioadmin'", catalog_text)
        self.assertNotIn("'fs.s3a.secret.key' = 'minioadmin123'", catalog_text)
        self.assertNotIn("'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'", catalog_text)
        self.assertNotIn("'s3.endpoint' = 'http://minio:9000'", catalog_text)
        self.assertNotIn("'s3.access-key-id' = 'minioadmin'", catalog_text)
        self.assertNotIn("'s3.secret-access-key' = 'minioadmin123'", catalog_text)
        self.assertIn("CREATE DATABASE IF NOT EXISTS lakehouse.analytics", catalog_text)
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.analytics.user_behavior_detail", catalog_text)
        self.assertIn("INSERT INTO lakehouse.analytics.user_behavior_detail", sink_text)
        self.assertIn("FROM user_behavior_source", sink_text)

    def test_chapter_5_runner_prepares_iceberg_jars_and_sql(self):
        text = CHAPTER5_RUNNER.read_text(encoding="utf-8")

        self.assertIn("function Invoke-CheckedCommand", text)
        self.assertIn("function Assert-DockerAvailable", text)
        self.assertIn("docker version", text)
        self.assertIn("iceberg-flink-runtime-1.19-1.6.1.jar", text)
        self.assertIn("iceberg-aws-bundle-1.6.1.jar", text)
        self.assertIn("hadoop-client-api-3.3.6.jar", text)
        self.assertIn("hadoop-client-runtime-3.3.6.jar", text)
        self.assertIn("hadoop-aws-3.3.6.jar", text)
        self.assertIn("aws-java-sdk-bundle-1.12.262.jar", text)
        self.assertIn("--profile flink --profile lakehouse up -d --force-recreate --quiet-pull", text)
        self.assertIn("06_create_iceberg_catalog.sql", text)
        self.assertIn("07_sink_user_behavior_to_iceberg.sql", text)
        self.assertIn('$combinedSqlFile = "tmp/chapter_5_flink_job.sql"', text)
        self.assertIn("docker exec $containerName /opt/flink/bin/sql-client.sh -f $containerSqlPath", text)

    def test_docs_mention_chapter_5_commands(self):
        minio_text = MINIO_README.read_text(encoding="utf-8")
        readme_text = README_FILE.read_text(encoding="utf-8")
        jobs_text = JOBS_README.read_text(encoding="utf-8")

        self.assertIn("MinIO", minio_text)
        self.assertIn("Iceberg", minio_text)
        self.assertIn("run_chapter_5_iceberg_pipeline.ps1", readme_text)
        self.assertIn("MinIO + Iceberg", readme_text)
        self.assertIn("Hive Metastore", readme_text)
        self.assertIn("06_create_iceberg_catalog.sql", jobs_text)
        self.assertIn("07_sink_user_behavior_to_iceberg.sql", jobs_text)
        self.assertIn("Hive Metastore", jobs_text)


if __name__ == "__main__":
    unittest.main()
