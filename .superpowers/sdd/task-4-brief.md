### Task 4: Document Chapter 6 commands and storyline

**Files:**
- Modify: `README.md`
- Modify: `jobs/README.md`
- Test: `tests/test_chapter_6_trino_artifacts.py`

**Interfaces:**
- Consumes: Chapter 6 command `./scripts/verify_chapter_6_trino_queries.ps1`
- Produces: discoverable documentation for Chapter 6 purpose, command usage, and place in the roadmap

- [ ] **Step 1: Expand tests with exact documentation expectations**

```python
    def test_readme_places_trino_after_iceberg(self) -> None:
        readme_text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

        self.assertIn("第 6 章：Trino + Iceberg 湖表查询", readme_text)
        self.assertIn("./scripts/verify_chapter_6_trino_queries.ps1", readme_text)
        self.assertIn("独立查询引擎", readme_text)
```

- [ ] **Step 2: Run the focused documentation test and verify it fails**

Run: `python -m unittest tests.test_chapter_6_trino_artifacts.Chapter6TrinoArtifactsTest.test_readme_places_trino_after_iceberg -v`

Expected: FAIL because Chapter 6 wording is not in the docs yet.

- [ ] **Step 3: Update `README.md`**

Add a Chapter 6 section with content like:

```markdown
## 第 6 章当前可用命令

### 做第 6 章 Trino + Iceberg 查询验证

```powershell
./scripts/verify_chapter_6_trino_queries.ps1
```

这一章的目标是把项目从“Flink 可以写入 Iceberg”推进到“Trino 这样的独立查询引擎也可以消费 Iceberg 湖表”。
```

Also update the chapter roadmap line to:

```markdown
7. 第 6 章：Trino + Iceberg 湖表查询
8. 第 7 章：ZooKeeper -> KRaft 架构演进
```

- [ ] **Step 4: Update `jobs/README.md`**

Add a Chapter 6 section with content like:

```markdown
## 第 6 章：Trino + Iceberg 湖表查询

- 新增 Trino 服务
- 继续复用 MinIO 上的 `lakehouse.analytics.user_behavior_detail`
- 通过 `11_trino_read_iceberg_user_behavior.sql` 验证总量查询和按 `event_type` 聚合
- 通过 `../scripts/verify_chapter_6_trino_queries.ps1` 做自动化验证
```

- [ ] **Step 5: Run artifact tests**

Run: `python -m unittest tests.test_chapter_6_trino_artifacts -v`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add README.md jobs/README.md tests/test_chapter_6_trino_artifacts.py
git commit -m "docs: add chapter 6 trino query guidance"
```

