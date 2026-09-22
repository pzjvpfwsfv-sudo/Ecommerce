# Olist 官方订单数据获取与规范化

## 目的

G2-E 使用 Olist 在 Kaggle 发布的 Brazilian E-Commerce Public Dataset Version 2。它是匿名化历史订单快照，不是实时订单 CDC，也不会与 REES46 行为数据强行关联身份。

正式来源：

- 数据主页：<https://www.kaggle.com/olistbr/brazilian-ecommerce/home>
- 元数据页：<https://www.kaggle.com/olistbr/brazilian-ecommerce/metadata>
- 发布组织：<https://www.kaggle.com/organizations/olistbr>

## 安全边界

- 归档、CSV、规范化 JSONL 和运行报告全部放在 `D:\EcommerceData\olist`。
- 不把 Kaggle 用户名、密码、API Token、Cookie 或下载链接中的临时凭据写入仓库和命令记录。
- Git 只保存工具、来源链接、口径和汇总证据，不保存第三方原始记录。
- 下载页面无法确认 Version 2、九文件集合或当前许可信息时停止，不改用镜像或二次清洗版。

## 获取步骤

1. 在浏览器打开官方主页并由数据使用者本人完成 Kaggle 登录。
2. 确认页面显示的数据版本、许可名称和许可链接，保留当次获取时间。
3. 将官方 ZIP 保存为 `D:\EcommerceData\olist\downloads\olist-brazilian-ecommerce.zip`。
4. 将 ZIP 原样解压到 `D:\EcommerceData\olist\raw\v2`，不要改文件名或编辑内容。
5. 确认目录中恰好存在九个官方 CSV，再运行规范化命令。

```powershell
$licenseName = Read-Host 'Kaggle 页面当前显示的许可名称'
$licenseUrl = Read-Host 'Kaggle 页面当前显示的许可链接'

python -m generators.olist_data prepare-bundle `
  --archive 'D:\EcommerceData\olist\downloads\olist-brazilian-ecommerce.zip' `
  --input-dir 'D:\EcommerceData\olist\raw\v2' `
  --output-root 'D:\EcommerceData\olist' `
  --acquired-at ((Get-Date).ToUniversalTime().ToString('o')) `
  --license-name $licenseName `
  --license-url $licenseUrl
```

成功后工具输出完整 manifest，并原子发布到：

```text
D:\EcommerceData\olist\prepared\<source_bundle_sha256>\source-bundle.json
D:\EcommerceData\olist\prepared\<source_bundle_sha256>\normalized\*.jsonl
```

`source_bundle_sha256` 是九个原文件内容、文件名、逻辑行数和逐文件摘要的共同身份。任何文件、行数或文件集合变化都会产生不同身份。已有正式目录不会被覆盖。

## 结果解释

- manifest 中的行数是 CSV 逻辑记录数，不包含表头；带换行的评价文本仍算一条记录。
- 金额同时保留源文本和两位定点规范列，后续计算只使用定点列。
- 源时间保持无时区语义，不追加 `Z`，不声称为 UTC。
- `source_row_id` 由数据集、官方文件名、逻辑记录号和规范行内容生成，可用于入湖对账，但不是 Olist 业务主键。
- 此步骤只证明本地文件与已登记官方归档一致；真实性陈述仍以 Olist 发布方说明为依据。
