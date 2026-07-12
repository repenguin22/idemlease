# idemlease v1.0.0 外部レビュー依頼プロンプト

以下をレビュアー（AI / 人間）にそのまま渡す。

---

あなたは Go・分散システム・HTTP セマンティクス・セキュリティに深い専門性を持つコードレビュアーです。公開直後のライブラリ **idemlease v1.0.0** を、広く使われる前の最終関門としてレビューしてください。目的は**賞賛ではなく欠陥の発見**です。正しさのバグ、契約違反、セキュリティホール、並行性の穴を最優先で探してください。

## 対象

- リポジトリ: https://github.com/repenguin22/idemlease （タグ `v1.0.0`、redistore は `redistore/v1.0.0`）
- 内容: HTTP API に Idempotency-Key 冪等性を追加する Go ライブラリ。コアはリースベースの状態機械（stdlib のみ・`net/http` 非依存）、`httpidem` が net/http ミドルウェア、`memstore`（開発用）/ `redistore`（Redis・Valkey、別 go.mod、Lua で原子操作）がストア実装
- 規模: 本体 約 1,700 行 + 公開適合性スイート 約 1,080 行 + テスト 25 ファイル
- Go 1.22 下限。ランタイム依存ゼロ（redistore のみ go-redis）

**正となる仕様**は `docs/REQUIREMENTS.md`（日本語、要件定義 v3.4。文中の `idemtrail` は `idemlease` に読み替え）。実装・README と契約が食い違う場合は**契約が正**であり、その食い違い自体が重大な指摘対象です。受け入れ条件は同文書 §12（14 項目）。

主要ファイル（優先度順）:

1. `idemlease.go` / `record.go` / `store.go` — コア状態機械（Begin/Finish、Store 契約、失効判定）
2. `httpidem/httpidem.go` — ミドルウェア本体（挙動表 §4.3、障害時挙動 §4.6）
3. `redistore/redistore.go` — Lua スクリプト 3 本による原子操作
4. `httpidem/recorder.go` — レスポンス捕捉と Flusher/Hijacker 透過
5. `httpidem/key.go` — キー文法（RFC 8941 互換パース含む）
6. `httpidem/storedresponse.go` — バージョンバイト付きバイナリ形式のデコーダ
7. `httpidem/policy.go` / `httpidem/errors.go` / `httpidem/fingerprint.go` / `memstore/memstore.go`
8. 公開スイート `idemleasetest/` と `httpidem/httpidemtest/` — テストが契約を本当に固定できているか

## レビュー観点（優先度順）

1. **状態機械の正しさ**: リース失効境界の TOCTOU、token CAS の穴、`LeaseLost` 正規化の取りこぼし、Begin の判定表（§3.1）と実装の乖離。「リース有効期間内の実行が高々 1 つ」という保証を破る実行順序を構成できるか試みてください
2. **redistore の Lua**: スクリプトの原子性、PEXPIRE（相対 TTL）と `lease_exp_ms`（アプリ時計の絶対値）の二重管理の破綻ケース、1ms クランプ、バイナリ安全性、クラスタでのスロット制約、EVALSHA/NOSCRIPT 周辺、go-redis の型変換
3. **HTTP セマンティクス**: キー文法のエッジケース（RFC 8941 パーサの逸脱、二重デコード、正規化差分による**キー衝突/分裂**）、指紋の曖昧性（EscapedPath vs RawPath、`?` の扱い、prefix 衝突）、捕捉の正しさ（1xx、暗黙 200、`Content-Length` と再生の整合、`http.ResponseController` 経由の Flush/Hijack）、body バッファリングと復元（バイパス経路の MultiReader 含む）
4. **セキュリティ**: KeyScope の分離破り（`scope+"\x00"+key` 合成の衝突可能性を含む）、リプレイによる情報漏洩（常時除外ヘッダ一覧の漏れはないか）、ログへのキー由来インジェクション、DoS（`MaxRequestBody`/`MaxResponseBody` の実効性、StoredResponse デコーダのアロケーション爆弾、memstore の無制限成長）、`bytes.Equal` による指紋比較の非定数時間性が問題になるか
5. **並行性**: `-race` が拾えない論理レース、`errBox`、Recorder の非スレッド安全性とハンドラ内 goroutine の組み合わせ、`context.WithoutCancel` した Finish の影響
6. **適合性スイートの網羅性**: §12 の 14 項目のうち、テストが実は固定できていないものはないか。スイート自体のバグ（false positive/negative）も対象
7. **API 設計・Go らしさ**: 命名、ゼロ値の扱い、sentinel エラー設計、godoc の正確さ、破壊的変更なしに v1.1（pgstore/CompleteTx、gRPC）へ拡張できるか
8. **ドキュメントと実装の乖離**: README（英語）・docs/README.ja.md・DESIGN.md の記述で、コードが実際にはそうなっていないもの

## 特に疑って見てほしい箇所（作者が自信を持ちきれていない点）

- `httpidem/recorder.go`: `captureWriter.WriteHeader` の 1xx 素通し + `wroteHeader` ラッチ。実サーバの 1xx（Expect: 100-continue、Early Hints）と組んだときの捕捉の正しさ
- `httpidem/httpidem.go`: body 全読み後の `r.ContentLength` は元のまま。復元 Body との不整合がハンドラを壊すケースはないか
- リプレイ応答: 保存時に `Content-Length`/`Content-Encoding` を allowlist 外として捨てるが、圧縮済みボディ（ハンドラが手動で gzip 書き込み + `Content-Encoding` ヘッダ）を保存→再生すると壊れないか
- `retryAfterSeconds` の Duration 整数演算、`StoredResponse.UnmarshalBinary` の巨大 uvarint / 切り詰めデータ耐性
- redistore: `Complete` の `rec_exp_ms`（アプリ時計）と PEXPIRE（Redis 時計）のズレが `Get`/`existing` 経由で `Record.Expired` 判定に与える影響
- `KeyScope` が返す scope 自体に `\x00` が含まれる場合の一意分解性（キー側は文法で NUL 拒否済み）

## 意図的な設計判断（既知。バグとして報告不要。ただし根拠付きの異議は歓迎）

- exactly-once は非目的（保証は「有効リース保持者が高々 1」のみ）。Discard は副作用の不在を意味しない
- draft-07 は参考のみ（失効済み）。生文字列キーが正規形で、RFC 8941 は互換受理
- `Require` のデフォルトは false（キーなし素通し）/ `Options` ゼロ値はデフォルト TTL（30s/24h）にフォールバック
- 失効境界は「期限ちょうど＝失効」/ reservation トークンは Begin 生成の 128bit hex
- Finish は `context.WithoutCancel`（クライアント切断で完了済み作業を失わないため）
- 5xx の problem detail はクライアントに出さない / 429・5xx・panic は Discard（panic は再 panic）
- Flusher/Hijacker はラッパーが常に実装し `http.ResponseController` 経由で委譲（非対応時は ErrNotSupported）— 「元 writer が実装する場合のみ透過」の解釈として採用
- redistore の go.mod は `require v1.0.0` + 開発用 `replace ../`（利用者には replace が無視される）
- テストヘルパー依存（go-cmp）は `_test.go` のみ可。本体と公開スイートは stdlib のみ（CI で機械検証）
- memstore は単一プロセス限定・lazy expiry（アクセスされないキーは残る）と明記済み

## 進め方と出力形式

検証コマンド: ルートで `go test -race ./...`。redistore は `REDIS_ADDR=localhost:6379 go test -race ./...`（Redis/Valkey が必要。未設定ならスキップ）。`e2e/echoe2e` と `docs/prototypes/errtrailadapter` は独立モジュール。

出力は以下の形式で:

1. **総評**（3〜5 文）と **Top 3 リスク**
2. **指摘一覧**（深刻度順: Critical / High / Medium / Low / Nit）。各指摘に必ず:
   - `ファイル:行` と、契約起因なら REQUIREMENTS の § 参照
   - **具体的な失敗シナリオ**（この入力・この実行順序 → この誤動作）。再現テストのスケッチがあれば理想
   - 修正案
3. **確認済みで問題なしの領域**も明示的に列挙（「見たが白」を可視化するため。上記観点 1〜8 それぞれについて）

推測で断定しないこと。コードを読んで確証が持てない指摘は「要確認」として深刻度と分離してください。v1.1 の新機能提案（pgstore、gRPC、Gin 等）はスコープ外です。

---

## 補足（依頼側メモ・プロンプトには含めなくてよい）

- リポジトリ非公開の相手やファイル添付が必要な場合の優先順: REQUIREMENTS.md → idemlease.go/record.go/store.go → httpidem/httpidem.go → redistore/redistore.go → recorder.go → key.go → storedresponse.go → 各スイート
- レビュー結果は Issue 化するか docs/review/ に記録し、Critical/High は v1.0.1 で対応する
