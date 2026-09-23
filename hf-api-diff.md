# Hugging Face API comparison

Hugging Face responses recorded from https://huggingface.co at 2026-09-22T18:47:21Z, replayed against http://127.0.0.1:18083.
Fixture repositories: model `wzshiming/gpt2`, dataset `wzshiming/fixtures_image_utils`, space `wzshiming/hello_world`; LFS file `64-8bits.tflite`.

Summary: 5 match, 20 content diff, 9 status diff, 0 capture error.

| Result | Case | HF | hfd | Diffs |
|:-------|------|----|-----|------:|
| ⚠️ content diff | agent-harnesses | 200 | 200 | 2 |
| ⚠️ content diff | datasets.commits | 200 | 200 | 6 |
| ⚠️ content diff | datasets.info | 200 | 200 | 9 |
| ❌ status diff | datasets.paths-info | 200 | 404 | 5 |
| ✅ match | datasets.refs | 200 | 200 | 0 |
| ❌ status diff | datasets.resolve.readme.head | 307 -> 200 | 200 | 11 |
| ⚠️ content diff | datasets.tree | 200 | 200 | 14 |
| ⚠️ content diff | datasets.tree.recursive | 200 | 200 | 49 |
| ✅ match | datasets.treesize | 200 | 200 | 0 |
| ⚠️ content diff | models.commits | 200 | 200 | 6 |
| ⚠️ content diff | models.commits.limit | 200 | 200 | 6 |
| ⚠️ content diff | models.info | 200 | 200 | 21 |
| ⚠️ content diff | models.list | 200 | 200 | 15 |
| ❌ status diff | models.notfound | 401 | 404 | 4 |
| ❌ status diff | models.paths-info | 200 | 404 | 5 |
| ✅ match | models.refs | 200 | 200 | 0 |
| ❌ status diff | models.resolve.config | 307 -> 200 | 200 | 9 |
| ❌ status diff | models.resolve.config.head | 307 -> 200 | 200 | 11 |
| ⚠️ content diff | models.resolve.lfs | 302 -> 200 | 302 -> 200 | 12 |
| ❌ status diff | models.resolve.lfs.head | 302 -> 200 | 200 | 19 |
| ⚠️ content diff | models.resolve.notfound | 404 | 404 | 6 |
| ⚠️ content diff | models.revision | 200 | 200 | 21 |
| ❌ status diff | models.revision.notfound | 404 | 200 | 15 |
| ⚠️ content diff | models.tree | 200 | 200 | 10 |
| ⚠️ content diff | models.tree.notfound | 404 | 404 | 3 |
| ⚠️ content diff | models.tree.recursive | 200 | 200 | 39 |
| ✅ match | models.treesize | 200 | 200 | 0 |
| ⚠️ content diff | models.xet-read-token | 200 | 200 | 1 |
| ⚠️ content diff | spaces.commits | 200 | 200 | 152 |
| ⚠️ content diff | spaces.info | 200 | 200 | 22 |
| ✅ match | spaces.refs | 200 | 200 | 0 |
| ❌ status diff | spaces.resolve.readme.head | 307 -> 200 | 200 | 11 |
| ⚠️ content diff | spaces.tree | 200 | 200 | 3 |
| ⚠️ content diff | whoami-v2 | 401 | 401 | 3 |

<details>
<summary>Comparison rules</summary>

- Recordings drop volatile headers, Xet access tokens and signed URL query values.
- Compared headers: Accept-Ranges, Cache-Control, Content-Disposition, Content-Type, Link, Location, Www-Authenticate, X-Error-Code, X-Error-Message, X-Linked-Etag, X-Linked-Size, X-Repo-Commit, X-Total-Count, X-Xet-Hash (Content-Type without charset), plus ETag and Content-Length for file and non-JSON responses.
- JSON bodies are compared structurally; the values of _id, accessToken, avatar, casUrl, createdAt, downloads, downloadsAllTime, exp, lastModified, likes, securityFileStatus, spaces, trendingScore, usedStorage are ignored while their presence and type are still compared, and arrays of objects are paired by path, rfilename, name, ref, id, oid when that key is unique on both sides.
- File payloads are compared by redirect chain and final SHA-256 digest.
- A case is a status diff when the status chains differ and a capture error when a request failed or a capture is truncated or invalid; neither counts as a match.

</details>

## Differences

In each hunk `-` is the Hugging Face value (expected) and `+` is the hfd value (actual); `<absent>` marks a side without that field.

<details>
<summary>⚠️ agent-harnesses — content diff (2 differences)</summary>

Request: `GET /api/agent-harnesses`

```diff
@@ response.headers.Accept-Ranges (only in hfd) @@
- <absent>
+ bytes
@@ response.json.harnesses.sandbase-harness (missing in hfd) @@
- {
-   "description": "Local-first, self-hosted runtime for persistent AI agent sessions and MCP tools.",
-   "docsUrl": "https://github.com/sandbaseai/sandbase-harness/blob/main/docs/installation.md",
-   "prettyLabel": "SandBase Harness",
-   "repoUrl": "https://github.com/sandbaseai/sandbase-harness"
- }
+ <absent>
```

</details>

<details>
<summary>⚠️ datasets.commits — content diff (6 differences)</summary>

Request: `GET /api/datasets/wzshiming/fixtures_image_utils/commits/main`

```diff
@@ response.headers.X-Total-Count (missing in hfd) @@
- 1
+ <absent>
@@ response.json[id=ba937e465960aa8dc41a6126458531ccc65cef76].authors (length differs) @@
- 2
+ 1
@@ response.json[id=ba937e465960aa8dc41a6126458531ccc65cef76].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/64e9ed3b233101ed99cd9030/pWD8oLjx4n7_AEem7faot.jpeg"
+ <absent>
@@ response.json[id=ba937e465960aa8dc41a6126458531ccc65cef76].authors[0].user (value differs) @@
- "wzshiming"
+ "Shiming Zhang"
@@ response.json[id=ba937e465960aa8dc41a6126458531ccc65cef76].authors[1] (missing in hfd) @@
- {
-   "avatar": "https://cdn-avatars.huggingface.co/v1/production/uploads/5dd96eb166059660ed1ee413/NQtzmrDdbG0H8qkZvRyGk.jpeg",
-   "user": "julien-c"
- }
+ <absent>
@@ response.json[id=ba937e465960aa8dc41a6126458531ccc65cef76].message (value differs) @@
- "\n\n\nCo-authored-by: Julien Chaumond \u003cjulien-c@users.noreply.huggingface.co\u003e\n"
+ "Duplicate from hf-internal-testing/fixtures_image_utils\n\n\nCo-authored-by: Julien Chaumond \u003cjulien-c@users.noreply.huggingface.co\u003e\n"
```

</details>

<details>
<summary>⚠️ datasets.info — content diff (9 differences)</summary>

Request: `GET /api/datasets/wzshiming/fixtures_image_utils`

```diff
@@ response.json._id (missing in hfd) @@
- "6ab2ba330a7ef9b9eb8ccc6a"
+ <absent>
@@ response.json.author (missing in hfd) @@
- "wzshiming"
+ <absent>
@@ response.json.cardData (only in hfd) @@
- <absent>
+ {}
@@ response.json.citation (missing in hfd) @@
- "\\\\n"
+ <absent>
@@ response.json.createdAt (missing in hfd) @@
- "2026-09-22T17:26:11.000Z"
+ <absent>
@@ response.json.description (missing in hfd) @@
- "\\\\n"
+ <absent>
@@ response.json.lastModified (missing in hfd) @@
- "2026-09-22T17:26:11.000Z"
+ <absent>
@@ response.json.tags (length differs) @@
- 1
+ 0
@@ response.json.tags[0] (missing in hfd) @@
- "region:us"
+ <absent>
```

</details>

<details>
<summary>❌ datasets.paths-info — status diff (5 differences)</summary>

Request: `POST /api/datasets/wzshiming/fixtures_image_utils/paths-info/main` with body `{"paths":["README.md","does-not-exist.txt"],"expand":true}`

```diff
@@ response.status (status differs) @@
- 200
+ 404
@@ response.headers.Content-Length (value differs) @@
- 606
+ 19
@@ response.headers.Content-Type (value differs) @@
- application/json; charset=utf-8
+ text/plain; charset=utf-8
@@ response.headers.Etag (missing in hfd) @@
- W/"25e-UEEPWinPv2NPAVeSkp61EX7E4XY"
+ <absent>
@@ response.body (type differs) @@
- json
+ 404 page not found
+
```

</details>

<details>
<summary>❌ datasets.resolve.readme.head — status diff (11 differences)</summary>

Request: `HEAD /datasets/wzshiming/fixtures_image_utils/resolve/main/README.md`

```diff
@@ response.status (status differs) @@
- 307 -> 200
+ 200
@@ response.headers.Accept-Ranges (missing in hfd) @@
- bytes
+ <absent>
@@ response.headers.Content-Disposition (value differs) @@
- inline; filename*=UTF-8''README.md; filename="README.md";
+ inline; filename=README.md
@@ response.headers.Content-Length (value differs) @@
- 273
+ 365
@@ response.headers.Content-Type (missing in hfd) @@
- text/plain; charset=utf-8
+ <absent>
@@ response.headers.Etag (only in hfd) @@
- <absent>
+ "072da2918ce4d084616646eca739fdc36b534a96"
@@ response.headers.Location (missing in hfd) @@
- /api/resolve-cache/datasets/wzshiming/fixtures_image_utils/ba937e465960aa8dc41a6126458531ccc65cef76/README.md?%2Fdatasets%2Fwzshiming%2Ffixtures_image_utils%2Fresolve%2Fmain%2FREADME.md=&etag=%22072da2918ce4d084616646eca739fdc36b534a96%22
+ <absent>
@@ response.headers.X-Linked-Etag (missing in hfd) @@
- "072da2918ce4d084616646eca739fdc36b534a96"
+ <absent>
@@ final.headers.Accept-Ranges (missing in hfd) @@
- bytes
+ <absent>
@@ final.headers.Content-Disposition (value differs) @@
- inline; filename*=UTF-8''README.md; filename="README.md";
+ inline; filename=README.md
@@ final.headers.Content-Type (missing in hfd) @@
- text/plain; charset=utf-8
+ <absent>
```

</details>

<details>
<summary>⚠️ datasets.tree — content diff (14 differences)</summary>

Request: `GET /api/datasets/wzshiming/fixtures_image_utils/tree/main`

```diff
@@ response.json[path=ai2d-demo-2.png].xetHash (missing in hfd) @@
- "628d66b7400a2ddea6cac1b824b11f459a4903ea0e370a11930712eb561798c5"
+ <absent>
@@ response.json[path=ai2d-demo.jpg].xetHash (missing in hfd) @@
- "d21fc316931892f4994c6d9474fc351953920f9dbe83c5f361bf138fe761b4b9"
+ <absent>
@@ response.json[path=australia.jpg].xetHash (missing in hfd) @@
- "bf3b1734cdae3e0d472d205332160479e05ce1d41d9513a03d6b1fc5a86d71d0"
+ <absent>
@@ response.json[path=bee.jpg].xetHash (missing in hfd) @@
- "90d86706dd89ba5541d5bc75acc38d0e1aa60ec2509651c2afbe38fa3f973544"
+ <absent>
@@ response.json[path=candy.JPG].xetHash (missing in hfd) @@
- "3e77b6e4aa4bf576015e653e6adeac4ae9507202c98c125edf513e27fbf1d81f"
+ <absent>
@@ response.json[path=car.png].xetHash (missing in hfd) @@
- "ba9f39b57bca33098e92294b8f6cf7e2196074cbee6777ba71a73d6012f16014"
+ <absent>
@@ response.json[path=coco_sample.png].xetHash (missing in hfd) @@
- "0d9116bd9024fc8c445534d31969ac4f2e0271ce9a3c61fd1e0f185846533f09"
+ <absent>
@@ response.json[path=compel-neg.png].xetHash (missing in hfd) @@
- "d807d8b3e26f086a1cbf318ea4ee899a4033b8a7046e005bdc6ce805b2cea5ab"
+ <absent>
@@ response.json[path=receipt_00008.png].xetHash (missing in hfd) @@
- "4feed501e2b50998c80ab5c0c19b5af954b00ad501d8e0a010c06369df1ab2df"
+ <absent>
@@ response.json[path=snowman.jpg].xetHash (missing in hfd) @@
- "9adc71509dcc3795628bd185b6eace1265ab285dc2377997b24c52bc4a9dc530"
+ <absent>
@@ response.json[path=snowman.png].xetHash (missing in hfd) @@
- "c8cf2d125474b1cae4d84a88a9439f6135588505ca8fad3d129aeb0f78e15935"
+ <absent>
@@ response.json[path=temple-bar-dublin-world-famous-irish-pub.jpg].xetHash (missing in hfd) @@
- "a2b729f43f9af9a285a12f83a34a40cf7b8944286a13b722b67c0550c2cce984"
+ <absent>
@@ response.json[path=unsplash_1552053831-71594a27632d.jpg].xetHash (missing in hfd) @@
- "21e49ef8fa6083b6c378634ac7ea13d0bbee7e10ae0dd3461b1982423d0b3bb6"
+ <absent>
@@ response.json[path=unsplash_1617258683320-61900b281ced.jpg].xetHash (missing in hfd) @@
- "20c55e2247bf6538ebb67c0652f0ac81c841297ae21f71a4d28d35cf2ced37f9"
+ <absent>
```

</details>

<details>
<summary>⚠️ datasets.tree.recursive — content diff (49 differences)</summary>

Request: `GET /api/datasets/wzshiming/fixtures_image_utils/tree/main?expand=true&recursive=true`

```diff
@@ response.json[path=.gitattributes].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "",
-     "status": "error"
-   },
-   "pickleImportScan": {
-     "status": "unscanned"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "error",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=PROVENANCE.md].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=README.md].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=ai2d-demo-2.png].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "File not scanned in VirusTotal",
-     "reportLink": "https://www.virustotal.com/gui/file/897b555f5767393e2f5da3bfad9deac8fb8a4af9d882ce1aff9b89234e0d513f?utm_source=huggingface",
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=ai2d-demo-2.png].xetHash (missing in hfd) @@
- "628d66b7400a2ddea6cac1b824b11f459a4903ea0e370a11930712eb561798c5"
+ <absent>
@@ response.json[path=ai2d-demo.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "File not scanned in VirusTotal",
-     "reportLink": "https://www.virustotal.com/gui/file/6d3a69a5d4fb0ceb97f76b4ac13f0c797c8a0ebe3f343265baee28bd981ecc3a?utm_source=huggingface",
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=ai2d-demo.jpg].xetHash (missing in hfd) @@
- "d21fc316931892f4994c6d9474fc351953920f9dbe83c5f361bf138fe761b4b9"
+ <absent>
@@ response.json[path=australia.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "File not scanned in VirusTotal",
-     "reportLink": "https://www.virustotal.com/gui/file/2070aff92b7dee3228fb49b6e2ba2557c53d9aedf3f7e1b01fac47c9755c1552?utm_source=huggingface",
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=australia.jpg].xetHash (missing in hfd) @@
- "bf3b1734cdae3e0d472d205332160479e05ce1d41d9513a03d6b1fc5a86d71d0"
+ <absent>
@@ response.json[path=bee.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "0/77 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/8b21ba78250f852ca5990063866b1ace6432521d0251bde7f8de783b22c99a6d?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=bee.jpg].xetHash (missing in hfd) @@
- "90d86706dd89ba5541d5bc75acc38d0e1aa60ec2509651c2afbe38fa3f973544"
+ <absent>
@@ response.json[path=candy.JPG].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "File not scanned in VirusTotal",
-     "reportLink": "https://www.virustotal.com/gui/file/fc417c899e94f8df465b7541c5a70f0eebb85c414d06345f0b290c061eccc84c?utm_source=huggingface",
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=candy.JPG].xetHash (missing in hfd) @@
- "3e77b6e4aa4bf576015e653e6adeac4ae9507202c98c125edf513e27fbf1d81f"
+ <absent>
@@ response.json[path=car.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=car.png].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "0/75 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/c6ae6440e3862dd9768cd9f7098b783ac980629dd7bc1fe69c10b39fca5a84cd?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=car.png].xetHash (missing in hfd) @@
- "ba9f39b57bca33098e92294b8f6cf7e2196074cbee6777ba71a73d6012f16014"
+ <absent>
@@ response.json[path=cat-rotated.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=cats.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=coco_sample.png].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "0/75 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/cf6f3c4befa148732c7453e0de5afab00f682427435fead2d88b07a9615cdac2?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=coco_sample.png].xetHash (missing in hfd) @@
- "0d9116bd9024fc8c445534d31969ac4f2e0271ce9a3c61fd1e0f185846533f09"
+ <absent>
@@ response.json[path=compel-neg.png].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "File not scanned in VirusTotal",
-     "reportLink": "https://www.virustotal.com/gui/file/ef279be66cb83fc26e12313a4ff830d4f24cbed4eb9f3882f7c2245b96fbcc86?utm_source=huggingface",
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=compel-neg.png].xetHash (missing in hfd) @@
- "d807d8b3e26f086a1cbf318ea4ee899a4033b8a7046e005bdc6ce805b2cea5ab"
+ <absent>
@@ response.json[path=fixtures_image_utils.py].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=lena.png].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=parrots.png].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=picsum_17_150x500.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "",
-     "status": "error"
-   },
-   "pickleImportScan": {
-     "status": "unscanned"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "error",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=picsum_231_200x300.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "status": "unscanned"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=picsum_237_200x200.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "",
-     "status": "safe",
-     "version": "1.5.4/28112"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=picsum_237_200x300.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "",
-     "status": "error"
-   },
-   "pickleImportScan": {
-     "status": "unscanned"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "error",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=picsum_237_400x300.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "",
-     "status": "safe",
-     "version": "1.5.4/28112"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=picsum_247_200x200.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "",
-     "status": "error"
-   },
-   "pickleImportScan": {
-     "status": "unscanned"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "error",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=pipeline-cat-chonk.jpeg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=provenance.jsonl].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=receipt_00008.png].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "0/77 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/58716693a6c0733358e66e64d7064a5a845424280bb55f9b5fa137b1af8c92fe?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=receipt_00008.png].xetHash (missing in hfd) @@
- "4feed501e2b50998c80ab5c0c19b5af954b00ad501d8e0a010c06369df1ab2df"
+ <absent>
@@ response.json[path=selena.jpeg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=snowman.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "",
-     "status": "error"
-   },
-   "pickleImportScan": {
-     "status": "unscanned"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "error",
-   "virusTotalScan": {
-     "message": "0/77 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/ec1e7dc759b7cfbbd70ecff48b712c2829e02ec37db92655d3285f048e274f18?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=snowman.jpg].xetHash (missing in hfd) @@
- "9adc71509dcc3795628bd185b6eace1265ab285dc2377997b24c52bc4a9dc530"
+ <absent>
@@ response.json[path=snowman.png].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "0/77 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/b97825997df04bd823207fd145331ffc3c3b62ec4e3a3adaac83c93debe87bdf?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=snowman.png].xetHash (missing in hfd) @@
- "c8cf2d125474b1cae4d84a88a9439f6135588505ca8fad3d129aeb0f78e15935"
+ <absent>
@@ response.json[path=temple-bar-dublin-world-famous-irish-pub.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "",
-     "status": "safe",
-     "version": "1.5.2/27952"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "File not scanned in VirusTotal",
-     "reportLink": "https://www.virustotal.com/gui/file/033627d104beb7efcad86cbe42d06d937d6d6ca49d413a59117513f422f5f431?utm_source=huggingface",
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=temple-bar-dublin-world-famous-irish-pub.jpg].xetHash (missing in hfd) @@
- "a2b729f43f9af9a285a12f83a34a40cf7b8944286a13b722b67c0550c2cce984"
+ <absent>
@@ response.json[path=tree.png].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=two_dogs.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=unsplash_1552053831-71594a27632d.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "status": "unscanned"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "File not scanned in VirusTotal",
-     "reportLink": "https://www.virustotal.com/gui/file/3e98bbbfd2b6c2c8427efa438c59d7191d06ae6ec9b595db76946a5b1bf1f2be?utm_source=huggingface",
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=unsplash_1552053831-71594a27632d.jpg].xetHash (missing in hfd) @@
- "21e49ef8fa6083b6c378634ac7ea13d0bbee7e10ae0dd3461b1982423d0b3bb6"
+ <absent>
@@ response.json[path=unsplash_1617258683320-61900b281ced.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "",
-     "status": "error"
-   },
-   "pickleImportScan": {
-     "status": "unscanned"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "error",
-   "virusTotalScan": {
-     "message": "File not scanned in VirusTotal",
-     "reportLink": "https://www.virustotal.com/gui/file/c1e745567911025b716ce943d4ef8734f94c829401298616e92bb182e48a5a24?utm_source=huggingface",
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=unsplash_1617258683320-61900b281ced.jpg].xetHash (missing in hfd) @@
- "20c55e2247bf6538ebb67c0652f0ac81c841297ae21f71a4d28d35cf2ced37f9"
+ <absent>
@@ response.json[path=vla_pi0.jpg].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "",
-     "status": "safe",
-     "version": "1.5.4/28112"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
```

</details>

<details>
<summary>⚠️ models.commits — content diff (6 differences)</summary>

Request: `GET /api/models/wzshiming/gpt2/commits/main`

```diff
@@ response.headers.X-Total-Count (missing in hfd) @@
- 1
+ <absent>
@@ response.json[id=ae329b6937f31305a0d6c6779f6cd4072cc4097e].authors (length differs) @@
- 2
+ 1
@@ response.json[id=ae329b6937f31305a0d6c6779f6cd4072cc4097e].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/64e9ed3b233101ed99cd9030/pWD8oLjx4n7_AEem7faot.jpeg"
+ <absent>
@@ response.json[id=ae329b6937f31305a0d6c6779f6cd4072cc4097e].authors[0].user (value differs) @@
- "wzshiming"
+ "Shiming Zhang"
@@ response.json[id=ae329b6937f31305a0d6c6779f6cd4072cc4097e].authors[1] (missing in hfd) @@
- {
-   "avatar": "https://cdn-avatars.huggingface.co/v1/production/uploads/5dd96eb166059660ed1ee413/NQtzmrDdbG0H8qkZvRyGk.jpeg",
-   "user": "julien-c"
- }
+ <absent>
@@ response.json[id=ae329b6937f31305a0d6c6779f6cd4072cc4097e].message (value differs) @@
- "\n\n\nCo-authored-by: Julien Chaumond \u003cjulien-c@users.noreply.huggingface.co\u003e\n"
+ "Duplicate from openai-community/gpt2\n\n\nCo-authored-by: Julien Chaumond \u003cjulien-c@users.noreply.huggingface.co\u003e\n"
```

</details>

<details>
<summary>⚠️ models.commits.limit — content diff (6 differences)</summary>

Request: `GET /api/models/wzshiming/gpt2/commits/main?limit=2`

```diff
@@ response.headers.X-Total-Count (missing in hfd) @@
- 1
+ <absent>
@@ response.json[id=ae329b6937f31305a0d6c6779f6cd4072cc4097e].authors (length differs) @@
- 2
+ 1
@@ response.json[id=ae329b6937f31305a0d6c6779f6cd4072cc4097e].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/64e9ed3b233101ed99cd9030/pWD8oLjx4n7_AEem7faot.jpeg"
+ <absent>
@@ response.json[id=ae329b6937f31305a0d6c6779f6cd4072cc4097e].authors[0].user (value differs) @@
- "wzshiming"
+ "Shiming Zhang"
@@ response.json[id=ae329b6937f31305a0d6c6779f6cd4072cc4097e].authors[1] (missing in hfd) @@
- {
-   "avatar": "https://cdn-avatars.huggingface.co/v1/production/uploads/5dd96eb166059660ed1ee413/NQtzmrDdbG0H8qkZvRyGk.jpeg",
-   "user": "julien-c"
- }
+ <absent>
@@ response.json[id=ae329b6937f31305a0d6c6779f6cd4072cc4097e].message (value differs) @@
- "\n\n\nCo-authored-by: Julien Chaumond \u003cjulien-c@users.noreply.huggingface.co\u003e\n"
+ "Duplicate from openai-community/gpt2\n\n\nCo-authored-by: Julien Chaumond \u003cjulien-c@users.noreply.huggingface.co\u003e\n"
```

</details>

<details>
<summary>⚠️ models.info — content diff (21 differences)</summary>

Request: `GET /api/models/wzshiming/gpt2`

```diff
@@ response.json._id (missing in hfd) @@
- "6ab2b9f6148b5cae56d6e842"
+ <absent>
@@ response.json.author (missing in hfd) @@
- "wzshiming"
+ <absent>
@@ response.json.config (missing in hfd) @@
- {
-   "architectures": [
-     "GPT2LMHeadModel"
-   ],
-   "model_type": "gpt2",
-   "tokenizer_config": {}
- }
+ <absent>
@@ response.json.createdAt (missing in hfd) @@
- "2026-09-22T17:25:10.000Z"
+ <absent>
@@ response.json.lastModified (missing in hfd) @@
- "2026-09-22T17:25:10.000Z"
+ <absent>
@@ response.json.model-index (missing in hfd) @@
- null
+ <absent>
@@ response.json.safetensors (missing in hfd) @@
- {
-   "parameters": {
-     "F32": 137022720
-   },
-   "total": 137022720
- }
+ <absent>
@@ response.json.spaces (missing in hfd) @@
- []
+ <absent>
@@ response.json.tags (length differs) @@
- 12
+ 4
@@ response.json.tags[0] (value differs) @@
- "pytorch"
+ "exbert"
@@ response.json.tags[1] (value differs) @@
- "tf"
+ "en"
@@ response.json.tags[2] (value differs) @@
- "jax"
+ "license:mit"
@@ response.json.tags[3] (value differs) @@
- "tflite"
+ "gpt2"
@@ response.json.tags[4] (missing in hfd) @@
- "rust"
+ <absent>
@@ response.json.tags[5] (missing in hfd) @@
- "onnx"
+ <absent>
@@ response.json.tags[6] (missing in hfd) @@
- "safetensors"
+ <absent>
@@ response.json.tags[7] (missing in hfd) @@
- "gpt2"
+ <absent>
@@ response.json.tags[8] (missing in hfd) @@
- "exbert"
+ <absent>
@@ response.json.tags[9] (missing in hfd) @@
- "en"
+ <absent>
@@ response.json.tags[10] (missing in hfd) @@
- "license:mit"
+ <absent>
@@ response.json.tags[11] (missing in hfd) @@
- "region:us"
+ <absent>
```

</details>

<details>
<summary>⚠️ models.list — content diff (15 differences)</summary>

Request: `GET /api/models?author=wzshiming&limit=5&search=gpt2`

```diff
@@ response.json[id=wzshiming/gpt2]._id (missing in hfd) @@
- "6ab2b9f6148b5cae56d6e842"
+ <absent>
@@ response.json[id=wzshiming/gpt2].createdAt (missing in hfd) @@
- "2026-09-22T17:25:10.000Z"
+ <absent>
@@ response.json[id=wzshiming/gpt2].tags (length differs) @@
- 12
+ 4
@@ response.json[id=wzshiming/gpt2].tags[0] (value differs) @@
- "pytorch"
+ "exbert"
@@ response.json[id=wzshiming/gpt2].tags[1] (value differs) @@
- "tf"
+ "en"
@@ response.json[id=wzshiming/gpt2].tags[2] (value differs) @@
- "jax"
+ "license:mit"
@@ response.json[id=wzshiming/gpt2].tags[3] (value differs) @@
- "tflite"
+ "gpt2"
@@ response.json[id=wzshiming/gpt2].tags[4] (missing in hfd) @@
- "rust"
+ <absent>
@@ response.json[id=wzshiming/gpt2].tags[5] (missing in hfd) @@
- "onnx"
+ <absent>
@@ response.json[id=wzshiming/gpt2].tags[6] (missing in hfd) @@
- "safetensors"
+ <absent>
@@ response.json[id=wzshiming/gpt2].tags[7] (missing in hfd) @@
- "gpt2"
+ <absent>
@@ response.json[id=wzshiming/gpt2].tags[8] (missing in hfd) @@
- "exbert"
+ <absent>
@@ response.json[id=wzshiming/gpt2].tags[9] (missing in hfd) @@
- "en"
+ <absent>
@@ response.json[id=wzshiming/gpt2].tags[10] (missing in hfd) @@
- "license:mit"
+ <absent>
@@ response.json[id=wzshiming/gpt2].tags[11] (missing in hfd) @@
- "region:us"
+ <absent>
```

</details>

<details>
<summary>❌ models.notfound — status diff (4 differences)</summary>

Request: `GET /api/models/wzshiming/does-not-exist`

```diff
@@ response.status (status differs) @@
- 401
+ 404
@@ response.headers.Www-Authenticate (missing in hfd) @@
- Bearer realm="Authentication required", charset="UTF-8"
+ <absent>
@@ response.headers.X-Error-Message (missing in hfd) @@
- Invalid username or password.
+ <absent>
@@ response.json.error (value differs) @@
- "Invalid username or password."
+ "repository \"wzshiming/does-not-exist\" not found"
```

</details>

<details>
<summary>❌ models.paths-info — status diff (5 differences)</summary>

Request: `POST /api/models/wzshiming/gpt2/paths-info/main` with body `{"paths":["config.json","64-8bits.tflite","does-not-exist.txt"],"expand":true}`

```diff
@@ response.status (status differs) @@
- 200
+ 404
@@ response.headers.Content-Length (value differs) @@
- 2195
+ 19
@@ response.headers.Content-Type (value differs) @@
- application/json; charset=utf-8
+ text/plain; charset=utf-8
@@ response.headers.Etag (missing in hfd) @@
- W/"893-I2D3KP3B+wd/122qriuEgUig9pg"
+ <absent>
@@ response.body (type differs) @@
- json
+ 404 page not found
+
```

</details>

<details>
<summary>❌ models.resolve.config — status diff (9 differences)</summary>

Request: `GET /wzshiming/gpt2/resolve/main/config.json`

Payload: identical (665 bytes, sha256:0daed7749b4f02b8f76240d5444551d7b08712dab4d0adb8239c56ba823bb7b4)

```diff
@@ response.status (status differs) @@
- 307 -> 200
+ 200
@@ response.headers.Accept-Ranges (missing in hfd) @@
- bytes
+ <absent>
@@ response.headers.Content-Disposition (only in hfd) @@
- <absent>
+ inline; filename=config.json
@@ response.headers.Content-Length (value differs) @@
- 232
+ 665
@@ response.headers.Etag (only in hfd) @@
- <absent>
+ "10c66461e4c109db5a2196bff4bb59be30396ed8"
@@ response.headers.Location (missing in hfd) @@
- /api/resolve-cache/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/config.json?%2Fwzshiming%2Fgpt2%2Fresolve%2Fmain%2Fconfig.json=&etag=%2210c66461e4c109db5a2196bff4bb59be30396ed8%22
+ <absent>
@@ response.headers.X-Linked-Etag (missing in hfd) @@
- "10c66461e4c109db5a2196bff4bb59be30396ed8"
+ <absent>
@@ final.headers.Accept-Ranges (missing in hfd) @@
- bytes
+ <absent>
@@ final.headers.Content-Disposition (value differs) @@
- inline; filename*=UTF-8''config.json; filename="config.json";
+ inline; filename=config.json
```

</details>

<details>
<summary>❌ models.resolve.config.head — status diff (11 differences)</summary>

Request: `HEAD /wzshiming/gpt2/resolve/main/config.json`

```diff
@@ response.status (status differs) @@
- 307 -> 200
+ 200
@@ response.headers.Accept-Ranges (missing in hfd) @@
- bytes
+ <absent>
@@ response.headers.Content-Disposition (value differs) @@
- inline; filename*=UTF-8''config.json; filename="config.json";
+ inline; filename=config.json
@@ response.headers.Content-Length (value differs) @@
- 232
+ 665
@@ response.headers.Content-Type (missing in hfd) @@
- text/plain; charset=utf-8
+ <absent>
@@ response.headers.Etag (only in hfd) @@
- <absent>
+ "10c66461e4c109db5a2196bff4bb59be30396ed8"
@@ response.headers.Location (missing in hfd) @@
- /api/resolve-cache/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/config.json?%2Fwzshiming%2Fgpt2%2Fresolve%2Fmain%2Fconfig.json=&etag=%2210c66461e4c109db5a2196bff4bb59be30396ed8%22
+ <absent>
@@ response.headers.X-Linked-Etag (missing in hfd) @@
- "10c66461e4c109db5a2196bff4bb59be30396ed8"
+ <absent>
@@ final.headers.Accept-Ranges (missing in hfd) @@
- bytes
+ <absent>
@@ final.headers.Content-Disposition (value differs) @@
- inline; filename*=UTF-8''config.json; filename="config.json";
+ inline; filename=config.json
@@ final.headers.Content-Type (missing in hfd) @@
- text/plain; charset=utf-8
+ <absent>
```

</details>

<details>
<summary>⚠️ models.resolve.lfs — content diff (12 differences)</summary>

Request: `GET /wzshiming/gpt2/resolve/main/64-8bits.tflite`

Payload: identical (125162496 bytes, sha256:c966da3b74697803352ca7c6f2f220e7090a557b619de9da0c6b34d89f7825c1)

```diff
@@ response.headers.Accept-Ranges (missing in hfd) @@
- bytes
+ <absent>
@@ response.headers.Cache-Control (missing in hfd) @@
- no-store
+ <absent>
@@ response.headers.Content-Disposition (only in hfd) @@
- <absent>
+ inline; filename=64-8bits.tflite
@@ response.headers.Content-Length (value differs) @@
- 994
+ 121
@@ response.headers.Content-Type (value differs) @@
- text/plain; charset=utf-8
+ text/html; charset=utf-8
@@ response.headers.Etag (only in hfd) @@
- <absent>
+ "c966da3b74697803352ca7c6f2f220e7090a557b619de9da0c6b34d89f7825c1"
@@ response.headers.Link (value differs) @@
- </api/models/wzshiming/gpt2/xet-read-token/ae329b6937f31305a0d6c6779f6cd4072cc4097e>; rel="xet-auth", <https://cas-server.xethub.hf.co/v1/reconstructions/f39b58dcd1ec07aaa95eb0c2f827a00dfac2404728b60d273ab3687f73311e03>; rel="xet-reconstruction-info"
+ </api/models/wzshiming/gpt2/xet-read-token/main>; rel="xet-auth", </v1/reconstructions/c91084eeb3b171c2ecfbe55a89383fc12af6c1d87b4bd238809bbdb048af0d0b>; rel="xet-reconstruction-info"
@@ response.headers.Location (value differs) @@
- https://us.aws.cdn.hf.co/xet-bridge-us/6ab2b9f6148b5cae56d6e842/f39b58dcd1ec07aaa95eb0c2f827a00dfac2404728b60d273ab3687f73311e03?Expires&Hash-Algorithm&Key-Pair-Id&Policy&Signature&X-Xet-Cas-Uid&response-content-disposition&user_id
+ /xet-bridge/c966da3b74697803352ca7c6f2f220e7090a557b619de9da0c6b34d89f7825c1
@@ response.headers.X-Xet-Hash (value differs) @@
- f39b58dcd1ec07aaa95eb0c2f827a00dfac2404728b60d273ab3687f73311e03
+ c91084eeb3b171c2ecfbe55a89383fc12af6c1d87b4bd238809bbdb048af0d0b
@@ hops[0].location (value differs) @@
- https://us.aws.cdn.hf.co/xet-bridge-us/6ab2b9f6148b5cae56d6e842/f39b58dcd1ec07aaa95eb0c2f827a00dfac2404728b60d273ab3687f73311e03?Expires&Hash-Algorithm&Key-Pair-Id&Policy&Signature&X-Xet-Cas-Uid&response-content-disposition&user_id
+ /xet-bridge/c966da3b74697803352ca7c6f2f220e7090a557b619de9da0c6b34d89f7825c1
@@ final.headers.Content-Disposition (missing in hfd) @@
- inline; filename*=UTF-8''64-8bits.tflite; filename="64-8bits.tflite";
+ <absent>
@@ final.headers.Etag (value differs) @@
- "f39b58dcd1ec07aaa95eb0c2f827a00dfac2404728b60d273ab3687f73311e03"
+ "c966da3b74697803352ca7c6f2f220e7090a557b619de9da0c6b34d89f7825c1"
```

</details>

<details>
<summary>❌ models.resolve.lfs.head — status diff (19 differences)</summary>

Request: `HEAD /wzshiming/gpt2/resolve/main/64-8bits.tflite`

```diff
@@ response.status (status differs) @@
- 302 -> 200
+ 200
@@ response.headers.Accept-Ranges (missing in hfd) @@
- bytes
+ <absent>
@@ response.headers.Cache-Control (missing in hfd) @@
- no-store
+ <absent>
@@ response.headers.Content-Disposition (only in hfd) @@
- <absent>
+ inline; filename=64-8bits.tflite
@@ response.headers.Content-Length (value differs) @@
- 998
+ 125162496
@@ response.headers.Content-Type (missing in hfd) @@
- text/plain; charset=utf-8
+ <absent>
@@ response.headers.Etag (only in hfd) @@
- <absent>
+ "c966da3b74697803352ca7c6f2f220e7090a557b619de9da0c6b34d89f7825c1"
@@ response.headers.Link (value differs) @@
- </api/models/wzshiming/gpt2/xet-read-token/ae329b6937f31305a0d6c6779f6cd4072cc4097e>; rel="xet-auth", <https://cas-server.xethub.hf.co/v1/reconstructions/f39b58dcd1ec07aaa95eb0c2f827a00dfac2404728b60d273ab3687f73311e03>; rel="xet-reconstruction-info"
+ </api/models/wzshiming/gpt2/xet-read-token/main>; rel="xet-auth", </v1/reconstructions/c91084eeb3b171c2ecfbe55a89383fc12af6c1d87b4bd238809bbdb048af0d0b>; rel="xet-reconstruction-info"
@@ response.headers.Location (missing in hfd) @@
- https://us.aws.cdn.hf.co/xet-bridge-us/6ab2b9f6148b5cae56d6e842/f39b58dcd1ec07aaa95eb0c2f827a00dfac2404728b60d273ab3687f73311e03?Expires&Hash-Algorithm&Key-Pair-Id&Policy&Signature&X-Xet-Cas-Uid&response-content-disposition&user_id
+ <absent>
@@ response.headers.X-Xet-Hash (value differs) @@
- f39b58dcd1ec07aaa95eb0c2f827a00dfac2404728b60d273ab3687f73311e03
+ c91084eeb3b171c2ecfbe55a89383fc12af6c1d87b4bd238809bbdb048af0d0b
@@ final.headers.Accept-Ranges (missing in hfd) @@
- bytes
+ <absent>
@@ final.headers.Content-Disposition (value differs) @@
- inline; filename*=UTF-8''64-8bits.tflite; filename="64-8bits.tflite";
+ inline; filename=64-8bits.tflite
@@ final.headers.Content-Type (missing in hfd) @@
- application/octet-stream
+ <absent>
@@ final.headers.Etag (value differs) @@
- "f39b58dcd1ec07aaa95eb0c2f827a00dfac2404728b60d273ab3687f73311e03"
+ "c966da3b74697803352ca7c6f2f220e7090a557b619de9da0c6b34d89f7825c1"
@@ final.headers.Link (only in hfd) @@
- <absent>
+ </api/models/wzshiming/gpt2/xet-read-token/main>; rel="xet-auth", </v1/reconstructions/c91084eeb3b171c2ecfbe55a89383fc12af6c1d87b4bd238809bbdb048af0d0b>; rel="xet-reconstruction-info"
@@ final.headers.X-Linked-Etag (only in hfd) @@
- <absent>
+ "c966da3b74697803352ca7c6f2f220e7090a557b619de9da0c6b34d89f7825c1"
@@ final.headers.X-Linked-Size (only in hfd) @@
- <absent>
+ 125162496
@@ final.headers.X-Repo-Commit (only in hfd) @@
- <absent>
+ ae329b6937f31305a0d6c6779f6cd4072cc4097e
@@ final.headers.X-Xet-Hash (only in hfd) @@
- <absent>
+ c91084eeb3b171c2ecfbe55a89383fc12af6c1d87b4bd238809bbdb048af0d0b
```

</details>

<details>
<summary>⚠️ models.resolve.notfound — content diff (6 differences)</summary>

Request: `GET /wzshiming/gpt2/resolve/main/does-not-exist.txt`

```diff
@@ response.headers.Content-Length (value differs) @@
- 15
+ 104
@@ response.headers.Content-Type (value differs) @@
- text/plain; charset=utf-8
+ application/json; charset=utf-8
@@ response.headers.Etag (missing in hfd) @@
- W/"f-mY2VvLxuxB7KhsoOdQTlMTccuAQ"
+ <absent>
@@ response.headers.X-Error-Code (missing in hfd) @@
- EntryNotFound
+ <absent>
@@ response.headers.X-Error-Message (missing in hfd) @@
- Entry not found
+ <absent>
@@ response.body (type differs) @@
- Entry not found
+ json
```

</details>

<details>
<summary>⚠️ models.revision — content diff (21 differences)</summary>

Request: `GET /api/models/wzshiming/gpt2/revision/main`

```diff
@@ response.json._id (missing in hfd) @@
- "6ab2b9f6148b5cae56d6e842"
+ <absent>
@@ response.json.author (missing in hfd) @@
- "wzshiming"
+ <absent>
@@ response.json.config (missing in hfd) @@
- {
-   "architectures": [
-     "GPT2LMHeadModel"
-   ],
-   "model_type": "gpt2",
-   "tokenizer_config": {}
- }
+ <absent>
@@ response.json.createdAt (missing in hfd) @@
- "2026-09-22T17:25:10.000Z"
+ <absent>
@@ response.json.lastModified (missing in hfd) @@
- "2026-09-22T17:25:10.000Z"
+ <absent>
@@ response.json.model-index (missing in hfd) @@
- null
+ <absent>
@@ response.json.safetensors (missing in hfd) @@
- {
-   "parameters": {
-     "F32": 137022720
-   },
-   "total": 137022720
- }
+ <absent>
@@ response.json.spaces (missing in hfd) @@
- []
+ <absent>
@@ response.json.tags (length differs) @@
- 12
+ 4
@@ response.json.tags[0] (value differs) @@
- "pytorch"
+ "exbert"
@@ response.json.tags[1] (value differs) @@
- "tf"
+ "en"
@@ response.json.tags[2] (value differs) @@
- "jax"
+ "license:mit"
@@ response.json.tags[3] (value differs) @@
- "tflite"
+ "gpt2"
@@ response.json.tags[4] (missing in hfd) @@
- "rust"
+ <absent>
@@ response.json.tags[5] (missing in hfd) @@
- "onnx"
+ <absent>
@@ response.json.tags[6] (missing in hfd) @@
- "safetensors"
+ <absent>
@@ response.json.tags[7] (missing in hfd) @@
- "gpt2"
+ <absent>
@@ response.json.tags[8] (missing in hfd) @@
- "exbert"
+ <absent>
@@ response.json.tags[9] (missing in hfd) @@
- "en"
+ <absent>
@@ response.json.tags[10] (missing in hfd) @@
- "license:mit"
+ <absent>
@@ response.json.tags[11] (missing in hfd) @@
- "region:us"
+ <absent>
```

</details>

<details>
<summary>❌ models.revision.notfound — status diff (15 differences)</summary>

Request: `GET /api/models/wzshiming/gpt2/revision/does-not-exist`

```diff
@@ response.status (status differs) @@
- 404
+ 200
@@ response.headers.X-Error-Code (missing in hfd) @@
- RevisionNotFound
+ <absent>
@@ response.headers.X-Error-Message (missing in hfd) @@
- Invalid rev id: does-not-exist
+ <absent>
@@ response.json.disabled (only in hfd) @@
- <absent>
+ false
@@ response.json.downloads (only in hfd) @@
- <absent>
+ 0
@@ response.json.error (missing in hfd) @@
- "Invalid rev id: does-not-exist"
+ <absent>
@@ response.json.gated (only in hfd) @@
- <absent>
+ false
@@ response.json.id (only in hfd) @@
- <absent>
+ "wzshiming/gpt2"
@@ response.json.likes (only in hfd) @@
- <absent>
+ 0
@@ response.json.modelId (only in hfd) @@
- <absent>
+ "wzshiming/gpt2"
@@ response.json.private (only in hfd) @@
- <absent>
+ false
@@ response.json.sha (only in hfd) @@
- <absent>
+ ""
@@ response.json.siblings (only in hfd) @@
- <absent>
+ null
@@ response.json.tags (only in hfd) @@
- <absent>
+ []
@@ response.json.usedStorage (only in hfd) @@
- <absent>
+ 5627942510
```

</details>

<details>
<summary>⚠️ models.tree — content diff (10 differences)</summary>

Request: `GET /api/models/wzshiming/gpt2/tree/main`

```diff
@@ response.json[path=] (only in hfd) @@
- <absent>
+ {
+   "oid": "",
+   "path": "",
+   "size": 0,
+   "type": ""
+ }
@@ response.json[path=64-8bits.tflite].xetHash (missing in hfd) @@
- "f39b58dcd1ec07aaa95eb0c2f827a00dfac2404728b60d273ab3687f73311e03"
+ <absent>
@@ response.json[path=64-fp16.tflite].xetHash (missing in hfd) @@
- "92939a77605871183fdf47c050d8604d430008e357fde3d9f77f192ac8d88730"
+ <absent>
@@ response.json[path=64.tflite].xetHash (missing in hfd) @@
- "5412078ab743d29acf18ca35ddba2c520ccc50049d9a62eaee9be9d4a4bb0d29"
+ <absent>
@@ response.json[path=flax_model.msgpack].xetHash (missing in hfd) @@
- "120fa10c9a116a074e005da769ce67c62b6e524424e630d068e00c23baa21d8d"
+ <absent>
@@ response.json[path=model.safetensors].xetHash (missing in hfd) @@
- "63bed80836ee0758c8fd4f8975d59bb0b864263ee2753547c358e8a37cde8758"
+ <absent>
@@ response.json[path=onnx] (missing in hfd) @@
- {
-   "oid": "d03ec5ec179df58241d27d55f92a674f5f44197f",
-   "path": "onnx",
-   "size": 0,
-   "type": "directory"
- }
+ <absent>
@@ response.json[path=pytorch_model.bin].xetHash (missing in hfd) @@
- "03d2dc814eb3c85f9acaf2ff2c5bb925c319f1032f61fca6f20ec2054839ef56"
+ <absent>
@@ response.json[path=rust_model.ot].xetHash (missing in hfd) @@
- "3b9acabb892fc72d4153e4b46de289370669d4f332e4c549ab740e6a4494f8f9"
+ <absent>
@@ response.json[path=tf_model.h5].xetHash (missing in hfd) @@
- "4195b41d9f36814bcbecf956b7e90bcbf41105ce9dd695a97246266b9328af55"
+ <absent>
```

</details>

<details>
<summary>⚠️ models.tree.notfound — content diff (3 differences)</summary>

Request: `GET /api/models/wzshiming/gpt2/tree/main/does-not-exist`

```diff
@@ response.headers.X-Error-Code (missing in hfd) @@
- EntryNotFound
+ <absent>
@@ response.headers.X-Error-Message (missing in hfd) @@
- does-not-exist does not exist on "main"
+ <absent>
@@ response.json.error (value differs) @@
- "does-not-exist does not exist on \"main\""
+ "failed to get tree for repo \"wzshiming/gpt2\" at rev \"main\" and path \"does-not-exist\": path not found: entry not found"
```

</details>

<details>
<summary>⚠️ models.tree.recursive — content diff (39 differences)</summary>

Request: `GET /api/models/wzshiming/gpt2/tree/main?expand=true&recursive=true`

```diff
@@ response.json[path=] (only in hfd) @@
- <absent>
+ {
+   "oid": "",
+   "path": "",
+   "size": 0,
+   "type": ""
+ }
@@ response.json[path=.gitattributes].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=602b71f15d40ed68c5f96330e3f3175a76a32126\u0026utm_source=huggingface",
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=64-8bits.tflite].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=90e59f00f62c654b1a88a5f127dff14df4611cdc\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "0/76 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/c966da3b74697803352ca7c6f2f220e7090a557b619de9da0c6b34d89f7825c1?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=64-8bits.tflite].xetHash (missing in hfd) @@
- "f39b58dcd1ec07aaa95eb0c2f827a00dfac2404728b60d273ab3687f73311e03"
+ <absent>
@@ response.json[path=64-fp16.tflite].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "",
-     "status": "queued"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=741986b58f10d46bb8c2e664da2be9f5aad7f9ea\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "queued",
-   "virusTotalScan": {
-     "message": "0/74 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/1ceafd82e733dd4b21570b2a86cf27556a983041806c033a55d086e0ed782cd3?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=64-fp16.tflite].xetHash (missing in hfd) @@
- "92939a77605871183fdf47c050d8604d430008e357fde3d9f77f192ac8d88730"
+ <absent>
@@ response.json[path=64.tflite].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "",
-     "status": "queued"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=ea581de86d796a5029e515ea575b9665782cc9d3\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "queued",
-   "virusTotalScan": {
-     "message": "0/75 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/cfcd510b239d90b71ee87d4e57a5a8c2d55b2a941e5d9fe5852298268ddbe61b?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=64.tflite].xetHash (missing in hfd) @@
- "5412078ab743d29acf18ca35ddba2c520ccc50049d9a62eaee9be9d4a4bb0d29"
+ <absent>
@@ response.json[path=README.md].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=a16a55fda99d2f2e7b69cce5cf93ff4ad3049930\u0026utm_source=huggingface",
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=config.json].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=10c66461e4c109db5a2196bff4bb59be30396ed8\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=flax_model.msgpack].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "",
-     "status": "queued"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=4d5d6829670cef6f69ea4c8bfee1c25fff1d9d95\u0026utm_source=huggingface",
-     "status": "unscanned"
-   },
-   "status": "queued",
-   "virusTotalScan": {
-     "message": "0/75 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/192e8257ae9e8f796f764630f4a488a6a16d1461762d62b49ef7405df951a283?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=flax_model.msgpack].xetHash (missing in hfd) @@
- "120fa10c9a116a074e005da769ce67c62b6e524424e630d068e00c23baa21d8d"
+ <absent>
@@ response.json[path=generation_config.json].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=3dc481ecc3b2c47a06ab4e20dba9d7f4b447bdf3\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=merges.txt].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=226b0752cac7789c48f0cb3ec53eda48b7be36cc\u0026utm_source=huggingface",
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=model.safetensors].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Safe model, does not support code execution on load.",
-     "reportLink": "https://research.jfrog.com/model-threats/noautoload-suscode?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=44b36d6e32d13c8fb28b0feab0ac8bfefa7efeda\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "0/75 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/248dfc3911869ec493c76e65bf2fcf7f615828b0254c12b473182f0f81d3a707?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=model.safetensors].xetHash (missing in hfd) @@
- "63bed80836ee0758c8fd4f8975d59bb0b864263ee2753547c358e8a37cde8758"
+ <absent>
@@ response.json[path=onnx] (missing in hfd) @@
- {
-   "lastCommit": {
-     "date": "2026-09-22T17:25:10.000Z",
-     "id": "ae329b6937f31305a0d6c6779f6cd4072cc4097e",
-     "title": "Duplicate from openai-community/gpt2"
-   },
-   "oid": "d03ec5ec179df58241d27d55f92a674f5f44197f",
-   "path": "onnx",
-   "size": 0,
-   "type": "directory"
- }
+ <absent>
@@ response.json[path=onnx/config.json].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=00bc2ad81dcdd043bf8490fdbd7cd82629fab389\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=onnx/decoder_model.onnx].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "",
-     "status": "queued"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=d1e0f6797a98e4710ae4c6928bd19440cf39bfbc\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "queued",
-   "virusTotalScan": {
-     "message": "0/75 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/e3fc9615868ff8f5e0429b892a0f6ca692784ba6c4ca31c4e9ee8218e7cce34f?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=onnx/decoder_model.onnx].xetHash (missing in hfd) @@
- "8e0a346a29483c54443f10b62662d77f82084cd3d8cde68b60013383754615be"
+ <absent>
@@ response.json[path=onnx/decoder_model_merged.onnx].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "",
-     "status": "queued"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=4e08d5f61950167c49b79778f789cefb63721505\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "queued",
-   "virusTotalScan": {
-     "message": "0/75 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/e6fc046fe5a7cfeeb8bb3c7d4c1b6a8bd90ced7339a75d5567957e3bc9d48abe?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=onnx/decoder_model_merged.onnx].xetHash (missing in hfd) @@
- "a961306242a93cd46d7bbb69b439d06a19d406feedca1e153e02e88a744cbfe1"
+ <absent>
@@ response.json[path=onnx/decoder_with_past_model.onnx].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "",
-     "status": "queued"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=27ce06e65620c241b5ae8eab8f6022812749a498\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "queued",
-   "virusTotalScan": {
-     "message": "0/74 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/570d958241de81f12d82a8358dfc0b408a7bf44ff2bd10ac4a97dab24a8118db?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=onnx/decoder_with_past_model.onnx].xetHash (missing in hfd) @@
- "a7960fdc3d3a34cc37d53200010e9371508b0af90594c9384088a6dcf28941af"
+ <absent>
@@ response.json[path=onnx/generation_config.json].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=30989900f4cebe446d28b25c9593001c7ef2853a\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=onnx/merges.txt].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=226b0752cac7789c48f0cb3ec53eda48b7be36cc\u0026utm_source=huggingface",
-     "status": "unscanned"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=onnx/special_tokens_map.json].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=0204ed10c186a4c7c68f55dff8f26087a45898d6\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=onnx/tokenizer.json].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=5de8effbe407429dce1f3ad068cd1bbd215d3a5c\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=onnx/tokenizer_config.json].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=546efe6d18ae2ad7758d0c9ef51cacdb81c8dc9d\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=onnx/vocab.json].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=84ef7fb594b5c0979e48bdeddb60a0adef33df0b\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=pytorch_model.bin].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Safe PyTorch model",
-     "reportLink": "https://research.jfrog.com/model-threats/pytorch-malcode?utm_source=huggingface",
-     "status": "safe"
-   },
-   "pickleImportScan": {
-     "pickleImports": [
-       {
-         "module": "collections",
-         "name": "OrderedDict",
-         "safety": "innocuous"
-       },
-       {
-         "module": "torch._utils",
-         "name": "_rebuild_tensor_v2",
-         "safety": "innocuous"
-       },
-       {
-         "module": "torch",
-         "name": "FloatStorage",
-         "safety": "innocuous"
-       }
-     ],
-     "status": "safe",
-     "version": "0.0.32"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=11f855591b3269c74652daee461f96fc238875ed\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "message": "0/76 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/7c5d3f4b8b76583b422fcb9189ad6c89d5d97a094541ce8932dce3ecabde1421?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=pytorch_model.bin].xetHash (missing in hfd) @@
- "03d2dc814eb3c85f9acaf2ff2c5bb925c319f1032f61fca6f20ec2054839ef56"
+ <absent>
@@ response.json[path=rust_model.ot].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "",
-     "status": "queued"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=cdec8ac7bc3c7e04961bce43fc494eccdc1fc8b8\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "queued",
-   "virusTotalScan": {
-     "message": "0/74 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/adf0adedbf4016b249550f866c66a3b3a3d09c8b3b3a1f6e5e9a265d94e0270e?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=rust_model.ot].xetHash (missing in hfd) @@
- "3b9acabb892fc72d4153e4b46de289370669d4f332e4c549ab740e6a4494f8f9"
+ <absent>
@@ response.json[path=tf_model.h5].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "",
-     "status": "queued"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=50a2bb89fbc2f5217ad429ff9605e9f1ab02f70f\u0026utm_source=huggingface",
-     "status": "unscanned"
-   },
-   "status": "queued",
-   "virusTotalScan": {
-     "message": "0/75 engines detect it as malicious.",
-     "reportLink": "https://www.virustotal.com/gui/file/d08c1307f7dfae6f878e0a2ca5715d587d2640530db8ef96fc0c1fc474dd9fee?utm_source=huggingface",
-     "status": "safe"
-   }
- }
+ <absent>
@@ response.json[path=tf_model.h5].xetHash (missing in hfd) @@
- "4195b41d9f36814bcbecf956b7e90bcbf41105ce9dd695a97246266b9328af55"
+ <absent>
@@ response.json[path=tokenizer.json].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=4b988bccc9dc5adacd403c00b4704976196548f8\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=tokenizer_config.json].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=be4d21d94f3b4687e5a54d84bf6ab46ed0f8defd\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
@@ response.json[path=vocab.json].securityFileStatus (missing in hfd) @@
- {
-   "avScan": {
-     "message": "No security issues detected",
-     "reportLink": "https://fdtn.ai/ai-supply-chain/hugging-face?utm_source=huggingface",
-     "status": "safe"
-   },
-   "jFrogScan": {
-     "message": "Not a machine-learning model",
-     "status": "unscanned"
-   },
-   "pickleImportScan": {
-     "pickleImports": [],
-     "status": "unscanned",
-     "version": "0.0.0"
-   },
-   "protectAiScan": {
-     "message": "This file has no security findings.",
-     "reportLink": "https://insights-db.paloaltonetworks.com/models/wzshiming/gpt2/ae329b6937f31305a0d6c6779f6cd4072cc4097e/files?blob-id=1f1d9aaca301414e7f6c9396df506798ff4eb9a6\u0026utm_source=huggingface",
-     "status": "safe"
-   },
-   "status": "safe",
-   "virusTotalScan": {
-     "status": "unscanned"
-   }
- }
+ <absent>
```

</details>

<details>
<summary>⚠️ models.xet-read-token — content diff (1 difference)</summary>

Request: `GET /api/models/wzshiming/gpt2/xet-read-token/main`

```diff
@@ response.headers.Cache-Control (only in hfd) @@
- <absent>
+ no-store
```

</details>

<details>
<summary>⚠️ spaces.commits — content diff (152 differences)</summary>

Request: `GET /api/spaces/wzshiming/hello_world/commits/main`

```diff
@@ response.headers.Link (value differs) @@
- </api/spaces/wzshiming/hello_world/commits/main?p=1&limit=50>; rel="next"
+ </api/spaces/wzshiming/hello_world/commits/main?limit=50&p=1>; rel="next"
@@ response.headers.X-Total-Count (missing in hfd) @@
- 676
+ <absent>
@@ response.json[id=04f3d3a0928da49c993b8fc9a96b843b13c6ce8f].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=04f3d3a0928da49c993b8fc9a96b843b13c6ce8f].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=04f3d3a0928da49c993b8fc9a96b843b13c6ce8f].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=0cbdc566bd746c6ede31d0a14fcc1a23667d9f16].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=0cbdc566bd746c6ede31d0a14fcc1a23667d9f16].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=0cbdc566bd746c6ede31d0a14fcc1a23667d9f16].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=0f59182316934a8f28724b8bf810df7b3015c654].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=0f59182316934a8f28724b8bf810df7b3015c654].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=0f59182316934a8f28724b8bf810df7b3015c654].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=1302de4af49cf616b7c61047cd232a9822a151ee].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=1302de4af49cf616b7c61047cd232a9822a151ee].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=1302de4af49cf616b7c61047cd232a9822a151ee].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=15f6b12d7a96b451cbe68eb05aa26d6d3afa1ca5].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=15f6b12d7a96b451cbe68eb05aa26d6d3afa1ca5].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=15f6b12d7a96b451cbe68eb05aa26d6d3afa1ca5].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=1f349570422dfd9990c7883b33600da494eda0bc].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=1f349570422dfd9990c7883b33600da494eda0bc].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=1f349570422dfd9990c7883b33600da494eda0bc].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=23b3ae2228c310f95ae609c20bb9329c49f71a67].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=23b3ae2228c310f95ae609c20bb9329c49f71a67].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=23b3ae2228c310f95ae609c20bb9329c49f71a67].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=295d9db68f3864e9a2a0a8a9576f49ba430c6ec1].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=295d9db68f3864e9a2a0a8a9576f49ba430c6ec1].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=295d9db68f3864e9a2a0a8a9576f49ba430c6ec1].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=2a86485315c1653a774c8476bc4da6aee8906c34].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=2a86485315c1653a774c8476bc4da6aee8906c34].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=2a86485315c1653a774c8476bc4da6aee8906c34].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=2d87c0d54e90bfe3f582437e1e74c5a94847559e].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=2d87c0d54e90bfe3f582437e1e74c5a94847559e].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=2d87c0d54e90bfe3f582437e1e74c5a94847559e].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=35616c537afe8aec380d0c37b25fae29576d667e].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=35616c537afe8aec380d0c37b25fae29576d667e].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=35616c537afe8aec380d0c37b25fae29576d667e].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=3879914a95638b197a88f85386e855fb0d8d5b95].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=3879914a95638b197a88f85386e855fb0d8d5b95].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=3879914a95638b197a88f85386e855fb0d8d5b95].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=39f9f7c662651f41aeb3ed5df21db40585d5c9e9].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=39f9f7c662651f41aeb3ed5df21db40585d5c9e9].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=39f9f7c662651f41aeb3ed5df21db40585d5c9e9].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=3c007dc6419c2df31d861bb774d9aa699056e1be].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=3c007dc6419c2df31d861bb774d9aa699056e1be].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=3c007dc6419c2df31d861bb774d9aa699056e1be].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=3c9cc5291768084490dd3f8ae9219dd4359c1531].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=3c9cc5291768084490dd3f8ae9219dd4359c1531].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=3c9cc5291768084490dd3f8ae9219dd4359c1531].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=3f01e8ae0b0d85035699b3c1127d9466b0edae0c].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=3f01e8ae0b0d85035699b3c1127d9466b0edae0c].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=3f01e8ae0b0d85035699b3c1127d9466b0edae0c].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=40c0939824e62372932610170e009163607c5597].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=40c0939824e62372932610170e009163607c5597].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=40c0939824e62372932610170e009163607c5597].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=42fb662bd733408fc277ebeeafbed0596deb9c26].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=42fb662bd733408fc277ebeeafbed0596deb9c26].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=42fb662bd733408fc277ebeeafbed0596deb9c26].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=43cba9017036d294a76072f31e5411f3b5048efc].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=43cba9017036d294a76072f31e5411f3b5048efc].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=43cba9017036d294a76072f31e5411f3b5048efc].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=551d323cc71b110fe53f6fe23c127d2bc783b5f2].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=551d323cc71b110fe53f6fe23c127d2bc783b5f2].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=551d323cc71b110fe53f6fe23c127d2bc783b5f2].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=5d1aaf4cdf954d341d4026b792121a2e66938e53].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=5d1aaf4cdf954d341d4026b792121a2e66938e53].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=5d1aaf4cdf954d341d4026b792121a2e66938e53].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=6051eb31c32109cbd6d530c2302851e20678ea42].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=6051eb31c32109cbd6d530c2302851e20678ea42].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=6051eb31c32109cbd6d530c2302851e20678ea42].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=7241d14e5d254ab5633e38ce107d89ed947e5d1c].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=7241d14e5d254ab5633e38ce107d89ed947e5d1c].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=7241d14e5d254ab5633e38ce107d89ed947e5d1c].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=72eb391fc2f4894e3e17da91255bd806c600351b].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=72eb391fc2f4894e3e17da91255bd806c600351b].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=72eb391fc2f4894e3e17da91255bd806c600351b].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=75c2a6742a85cf7325888c7ab58c2a041f473355].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=75c2a6742a85cf7325888c7ab58c2a041f473355].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=75c2a6742a85cf7325888c7ab58c2a041f473355].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=839255a1583f5e7881b3fdec55dd0fbd3653411f].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=839255a1583f5e7881b3fdec55dd0fbd3653411f].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=839255a1583f5e7881b3fdec55dd0fbd3653411f].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=8635b34aa4b20a7c2a4f7180b845626719a51163].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=8635b34aa4b20a7c2a4f7180b845626719a51163].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=8635b34aa4b20a7c2a4f7180b845626719a51163].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=869a21b1a0b9092d2aa15424f4fe67b686f5b05c].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=869a21b1a0b9092d2aa15424f4fe67b686f5b05c].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=869a21b1a0b9092d2aa15424f4fe67b686f5b05c].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=8d7350707282687cb14ae6ffecc7b813c2ddef4d].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=8d7350707282687cb14ae6ffecc7b813c2ddef4d].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=8d7350707282687cb14ae6ffecc7b813c2ddef4d].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=927916ba35d4be6b275e42365042e2ccff1ec112].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=927916ba35d4be6b275e42365042e2ccff1ec112].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=927916ba35d4be6b275e42365042e2ccff1ec112].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=9fbf3b8813c2ea9bc712939d4a82b20668985f36].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=9fbf3b8813c2ea9bc712939d4a82b20668985f36].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=9fbf3b8813c2ea9bc712939d4a82b20668985f36].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=9feb06c81463e6d5c520998070150701e65c6499].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=9feb06c81463e6d5c520998070150701e65c6499].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=9feb06c81463e6d5c520998070150701e65c6499].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=a1f3338f1df52cca923dc0c810ec9f13320d6ea3].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=a1f3338f1df52cca923dc0c810ec9f13320d6ea3].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=a1f3338f1df52cca923dc0c810ec9f13320d6ea3].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=a30c11da00cf003d2a0b8db6dc0dbebc486a3825].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=a30c11da00cf003d2a0b8db6dc0dbebc486a3825].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=a30c11da00cf003d2a0b8db6dc0dbebc486a3825].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=a7e9db7644db7fbdc94c77c85ab4f28759df6873].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=a7e9db7644db7fbdc94c77c85ab4f28759df6873].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=a7e9db7644db7fbdc94c77c85ab4f28759df6873].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=a8cc42408c6c2ed7d77fd62607bad50d6b669bd7].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=a8cc42408c6c2ed7d77fd62607bad50d6b669bd7].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=a8cc42408c6c2ed7d77fd62607bad50d6b669bd7].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=aeb41f2f35d95fa7f20dbabdff8702cf2d43c3aa].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=aeb41f2f35d95fa7f20dbabdff8702cf2d43c3aa].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=aeb41f2f35d95fa7f20dbabdff8702cf2d43c3aa].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=b6f09882c8c35cba3bf2b6219d416ec6b688b550].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=b6f09882c8c35cba3bf2b6219d416ec6b688b550].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=b6f09882c8c35cba3bf2b6219d416ec6b688b550].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=b9c3d04c03433227e913dc6d3c93e8f790359a99].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=b9c3d04c03433227e913dc6d3c93e8f790359a99].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=b9c3d04c03433227e913dc6d3c93e8f790359a99].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=c0381b9b30e9cdeaa5ed1fe1165ec3bda4a788ba].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=c0381b9b30e9cdeaa5ed1fe1165ec3bda4a788ba].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=c0381b9b30e9cdeaa5ed1fe1165ec3bda4a788ba].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=c08cc0e22e6393c17f92415c35b7eba5c5847b4b].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=c08cc0e22e6393c17f92415c35b7eba5c5847b4b].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=c08cc0e22e6393c17f92415c35b7eba5c5847b4b].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=c4ee11c90c883cd92c092e0688c3c1879e4618d0].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=c4ee11c90c883cd92c092e0688c3c1879e4618d0].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=c4ee11c90c883cd92c092e0688c3c1879e4618d0].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=c6908fc552709fb373d30e968639455598918fd8].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=c6908fc552709fb373d30e968639455598918fd8].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=c6908fc552709fb373d30e968639455598918fd8].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=e8e8183a808ae2af591aaff17191ac12a1f1b016].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=e8e8183a808ae2af591aaff17191ac12a1f1b016].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=e8e8183a808ae2af591aaff17191ac12a1f1b016].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=ec48796602a656c161a8cc8710791ddc08949ca4].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=ec48796602a656c161a8cc8710791ddc08949ca4].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=ec48796602a656c161a8cc8710791ddc08949ca4].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=ef7146284a2de360019a23b9d9298b9d578ea354].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=ef7146284a2de360019a23b9d9298b9d578ea354].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=ef7146284a2de360019a23b9d9298b9d578ea354].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=f150d930dc5a726b0410f1982bfe4f1a999f7f11].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=f150d930dc5a726b0410f1982bfe4f1a999f7f11].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=f150d930dc5a726b0410f1982bfe4f1a999f7f11].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=f3558536268699159d30bf8cea88a664c40fa646].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=f3558536268699159d30bf8cea88a664c40fa646].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=f3558536268699159d30bf8cea88a664c40fa646].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=f9b5224db2af58a6fa8c90b153f1241bad0f7652].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/1654278567459-626a9bfa03e2e2796f24ca11.jpeg"
+ <absent>
@@ response.json[id=f9b5224db2af58a6fa8c90b153f1241bad0f7652].authors[0].user (value differs) @@
- "freddyaboulton"
+ "Freddy Boulton"
@@ response.json[id=f9b5224db2af58a6fa8c90b153f1241bad0f7652].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
@@ response.json[id=fc00acaae2d665872d0594a9eac0266f158a30c5].authors[0].avatar (missing in hfd) @@
- "https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"
+ <absent>
@@ response.json[id=fc00acaae2d665872d0594a9eac0266f158a30c5].authors[0].user (value differs) @@
- "gradio-pr-bot"
+ "Gradio PR Bot"
@@ response.json[id=fc00acaae2d665872d0594a9eac0266f158a30c5].message (value differs) @@
- ""
+ "Upload folder using huggingface_hub"
```

</details>

<details>
<summary>⚠️ spaces.info — content diff (22 differences)</summary>

Request: `GET /api/spaces/wzshiming/hello_world`

```diff
@@ response.json._id (missing in hfd) @@
- "6ab2b972340accf53b97a784"
+ <absent>
@@ response.json.author (missing in hfd) @@
- "wzshiming"
+ <absent>
@@ response.json.cardData.app_file (missing in hfd) @@
- "run.py"
+ <absent>
@@ response.json.cardData.colorFrom (missing in hfd) @@
- "indigo"
+ <absent>
@@ response.json.cardData.colorTo (missing in hfd) @@
- "indigo"
+ <absent>
@@ response.json.cardData.emoji (missing in hfd) @@
- "🔥"
+ <absent>
@@ response.json.cardData.hf_oauth (missing in hfd) @@
- true
+ <absent>
@@ response.json.cardData.pinned (missing in hfd) @@
- false
+ <absent>
@@ response.json.cardData.sdk (missing in hfd) @@
- "gradio"
+ <absent>
@@ response.json.cardData.sdk_version (missing in hfd) @@
- "6.28.0"
+ <absent>
@@ response.json.cardData.title (missing in hfd) @@
- "hello_world"
+ <absent>
@@ response.json.createdAt (missing in hfd) @@
- "2026-09-22T17:22:58.000Z"
+ <absent>
@@ response.json.downloads (only in hfd) @@
- <absent>
+ 0
@@ response.json.host (missing in hfd) @@
- "https://wzshiming-hello-world.hf.space"
+ <absent>
@@ response.json.lastModified (missing in hfd) @@
- "2026-09-18T12:04:43.000Z"
+ <absent>
@@ response.json.region (missing in hfd) @@
- "us"
+ <absent>
@@ response.json.runtime (missing in hfd) @@
- {
-   "devMode": false,
-   "domains": [
-     {
-       "domain": "wzshiming-hello-world.hf.space",
-       "stage": "READY"
-     }
-   ],
-   "errorMessage": "No @spaces.GPU function detected during startup",
-   "gcTimeout": 172800,
-   "hardware": {
-     "current": null,
-     "requested": "zero-a10g"
-   },
-   "replicas": {
-     "requested": 1
-   },
-   "stage": "RUNTIME_ERROR"
- }
+ <absent>
@@ response.json.sdk (missing in hfd) @@
- "gradio"
+ <absent>
@@ response.json.subdomain (missing in hfd) @@
- "wzshiming-hello-world"
+ <absent>
@@ response.json.tags (length differs) @@
- 2
+ 0
@@ response.json.tags[0] (missing in hfd) @@
- "gradio"
+ <absent>
@@ response.json.tags[1] (missing in hfd) @@
- "region:us"
+ <absent>
```

</details>

<details>
<summary>❌ spaces.resolve.readme.head — status diff (11 differences)</summary>

Request: `HEAD /spaces/wzshiming/hello_world/resolve/main/README.md`

```diff
@@ response.status (status differs) @@
- 307 -> 200
+ 200
@@ response.headers.Accept-Ranges (missing in hfd) @@
- bytes
+ <absent>
@@ response.headers.Content-Disposition (value differs) @@
- inline; filename*=UTF-8''README.md; filename="README.md";
+ inline; filename=README.md
@@ response.headers.Content-Length (value differs) @@
- 251
+ 153
@@ response.headers.Content-Type (missing in hfd) @@
- text/plain; charset=utf-8
+ <absent>
@@ response.headers.Etag (only in hfd) @@
- <absent>
+ "d7f62e0ef31fcef4992a7c193e33354dbff551a3"
@@ response.headers.Location (missing in hfd) @@
- /api/resolve-cache/spaces/wzshiming/hello_world/2d87c0d54e90bfe3f582437e1e74c5a94847559e/README.md?%2Fspaces%2Fwzshiming%2Fhello_world%2Fresolve%2Fmain%2FREADME.md=&etag=%22d7f62e0ef31fcef4992a7c193e33354dbff551a3%22
+ <absent>
@@ response.headers.X-Linked-Etag (missing in hfd) @@
- "d7f62e0ef31fcef4992a7c193e33354dbff551a3"
+ <absent>
@@ final.headers.Accept-Ranges (missing in hfd) @@
- bytes
+ <absent>
@@ final.headers.Content-Disposition (value differs) @@
- inline; filename*=UTF-8''README.md; filename="README.md";
+ inline; filename=README.md
@@ final.headers.Content-Type (missing in hfd) @@
- text/plain; charset=utf-8
+ <absent>
```

</details>

<details>
<summary>⚠️ spaces.tree — content diff (3 differences)</summary>

Request: `GET /api/spaces/wzshiming/hello_world/tree/main`

```diff
@@ response.json[path=] (only in hfd) @@
- <absent>
+ {
+   "oid": "",
+   "path": "",
+   "size": 0,
+   "type": ""
+ }
@@ response.json[path=__pycache__] (missing in hfd) @@
- {
-   "oid": "93126891cbfacfce960a125dce9e42bec6a11753",
-   "path": "__pycache__",
-   "size": 0,
-   "type": "directory"
- }
+ <absent>
@@ response.json[path=screenshot.gif].xetHash (missing in hfd) @@
- "8de88a9afedbbf7fc8c6e94cd11c61bb1fcfb96463d7e99b3d52aa0ea863787c"
+ <absent>
```

</details>

<details>
<summary>⚠️ whoami-v2 — content diff (3 differences)</summary>

Request: `GET /api/whoami-v2`

```diff
@@ response.headers.Www-Authenticate (missing in hfd) @@
- Bearer realm="Authentication required", charset="UTF-8"
+ <absent>
@@ response.headers.X-Error-Message (missing in hfd) @@
- Invalid username or password.
+ <absent>
@@ response.json.error (value differs) @@
- "Invalid username or password."
+ "Unauthorized"
```

</details>
