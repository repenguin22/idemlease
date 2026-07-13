# idemlease（日本語版 README）

英語版（正）: [../README.md](../README.md)

Go の HTTP API に Idempotency-Key セマンティクスを追加するミドルウェア。クライアントは POST/PATCH を安全にリトライでき、重複リクエストには保存済みレスポンスが返る。コアの状態機械は依存ゼロ（stdlib のみ・`net/http` 非依存）でストア非依存、すべての統合が同一のコアを共有する。

> **保証の言明**: 同一キー（スコープ内）に対して**有効なリースを保持する実行は常に高々 1 つ**。exactly-once は保証しない — リース失効後の再実行や、結果の保存（Complete）失敗時のリトライにより、ハンドラが複数回実行されることはありうる。より強い保証（業務トランザクションとの原子的合流）は v1.1 の `pgstore` + `CompleteTx` で提供予定。

セマンティクスは決済系 API（Stripe、Adyen、WorldPay）で実証されたパターンに従う: 初見で予約、完了後はリプレイ、処理中の重複は 409、ペイロード不一致は 422。

## クイックスタート

```
go get github.com/repenguin22/idemlease
```

```go
mw := httpidem.New(memstore.New(), // 開発用 — 下記「ストア」参照
	httpidem.Require(true), // キーなしの POST/PATCH を 400 で拒否
)
http.ListenAndServe(":8080", mw(mux))
```

本番は Redis ストア（別モジュール、Redis 6.0+ / Valkey 対応、クラスタ安全）:

```
go get github.com/repenguin22/idemlease/redistore
```

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
mw := httpidem.New(redistore.New(client), httpidem.Require(true))
```

## クライアントから見た挙動

| リクエスト | レスポンス |
|---|---|
| キー付きの初回リクエスト | ハンドラ実行、レスポンス保存 |
| 再送（同一キー・同一内容） | 保存済みレスポンス + `Idempotency-Replayed: true`（ハンドラは再実行**されない**） |
| 初回が処理中の同一キー | `409 Conflict` + `Retry-After`（ペイロード問わず） |
| 完了後の同一キー・**異なる**ペイロード | `422 Unprocessable Entity` |
| キーなし（`Require(true)` 時） | `400 Bad Request`（デフォルトの `Require(false)` では素通し） |
| 不正なキー | `400 Bad Request` |
| ストア到達不能 | `503 Service Unavailable`（「障害時の意味論」参照） |

拒否応答は RFC 9457 の最小 `application/problem+json`。`httpidem.Errors(ErrorWriter)` で差し替え可能。sentinel（`ErrInFlight` / `ErrFingerprintMismatch` / `ErrKeyMissing` / `ErrKeyInvalid` 等）は `errors.Is` で判別できる。

## キー

正規形は**非引用の生文字列**: 前後の空白を除いた 1〜255 バイトの任意バイト列（制御文字を除く。内部空白・UTF-8 可）。互換のため RFC 8941 の引用符付き文字列（`"abc"`）も受理し、生形式の `abc` と**同一キー**として扱う。既知のトレードオフ: `"` で始まる生キーは引用形式で送る必要がある。UUID 限定などの追加制約は `httpidem.KeyValidator` で。

「同一リクエスト」の判定は**指紋**の一致: `SHA-256(メソッド + "\n" + パス + "?" + クエリ + "\n" + ボディ)`。Host とヘッダは意図的に含めない。

## セキュリティ: KeyScope（強く推奨）

キーはデフォルトでグローバル。マルチテナント API では、クライアント A が B と同じキーを送ると **B の保存済みレスポンスが A に返る**漏洩と、409/422 による他者キーの使用状況観測が可能になる。複数の呼び出し元にサービスするなら `KeyScope` の設定を**強く推奨**する:

```go
httpidem.KeyScope(func(r *http.Request) string {
	return tenantIDFromAuth(r) // 認証済みアカウント ID 等
})
```

スコープは内部で `scope + "\x00" + key` としてキーに合成され、テナント間で衝突せず、各テナント内では通常のリプレイ/409/422 が成立する。

## 何が保存・リプレイされるか

ステータスコード + allowlist ヘッダ + ボディ（`MaxResponseBody`、既定 1 MiB まで）。ヘッダ allowlist の既定は `Content-Type` / `Content-Language` / `Content-Encoding` / `Content-Disposition` / `Location`（`StoreHeaders(...)` で変更可。ただしボディの解釈に必要なヘッダ — 特に `Content-Encoding` — を外すと、圧縮ボディがラベルなしでリプレイされ壊れる）。**`Set-Cookie`・`Authorization`・hop-by-hop ヘッダは allowlist に載せてもリプレイされない** — 他クライアントへのセッション漏洩を防ぐため。

圧縮は idemlease の**外側**に圧縮ミドルウェアを置くのを推奨（リプレイ含め毎回リクエストに応じて圧縮される）。ハンドラが圧縮済みボディを直接書く場合はそのまま保存・リプレイされる — 異なる `Accept-Encoding` での再送にも保存時のエンコーディングが返る点に注意。

保存されないもの（クライアントには届いたうえで破棄）: ストリーミング応答（`Flush`/`Hijack` 時点で捕捉放棄）、`MaxResponseBody` 超過、リプレイポリシーが Discard としたもの。

ハンドラは return 前に書き込みを終えること。net/http 自体と同様、ハンドラより長生きする goroutine からの書き込みに対して捕捉は安全でない。

## リプレイポリシー

既定はステータス駆動: 2xx/3xx は Persist、4xx は Persist（再送に同じエラーを返す）、**429 は Discard**（保存すると制限解除後も永遠に 429 が返るため）、5xx は Discard、panic は Discard + 再 panic。何も書かないハンドラは 200 扱い。失敗理由で判定したい場合はハンドラで `httpidem.SetError(ctx, err)` を呼び、カスタム `Policy` を与える。

## 障害時の意味論（本番投入前に必読）

- **Reserve 失敗（ストアダウン）**: 既定は fail-closed の `503`。`FailOpen(true)` で「**冪等性なしの**素通し + 警告ログ」に変更可。どちらのリスクを取るか明示的に選ぶこと
- **Persist 失敗（実行後にストアダウン）**: クライアントにはレスポンスが返るが保存はされない。**冪等性が一時的に弱まる**: ベストエフォートの Release により再送は再実行でき、Release も失敗した場合はリース失効まで 409、失効後に再実行
- **Discard は「副作用なし」を意味しない**: Release は再実行を許可するだけで、既に起きた副作用はそのまま。二重課金が許されない処理はハンドラ側でも防御するか、v1.1 のトランザクション合流を待つこと
- **長時間実行中のリース失効**: 失効後に再送がキーを再予約した場合、元の実行のレスポンスはそのクライアントに返るが、保存・リプレイされるのは**新しい実行の結果のみ**（`lease_lost` 警告ログ）。`LeaseTTL`（既定 30 秒）は最遅ハンドラより長く設定すること

ログは `slog` に出力され、全行に `idempotency_key`（KeyScope 使用時はスコープも）が付く。キーに秘密が含まれうる場合は `HashKeysInLogs(true)` を。

## ストア

| ストア | モジュール | 用途 |
|---|---|---|
| `memstore` | （本モジュール） | 開発・テスト用。**単一プロセス限定** — ロードバランサ配下では保証が成立しない |
| `redistore` | `github.com/repenguin22/idemlease/redistore` | 本番用。Redis 6.0+ / 互換実装（Valkey は CI で常時検証）。Lua による原子的 reserve/complete/release、クラスタ安全 |
| `pgstore` | 予定（v1.1） | PostgreSQL + 業務トランザクションとの原子的合流 |

自作ストアも歓迎: `idemlease.Store`（4 メソッド）を実装し、同梱の適合性スイート（`idemleasetest.RunStoreTests` / `RunStateMachineTests` / `httpidemtest.RunHTTPTests`、いずれも stdlib のみ）で検証できる。

## フレームワーク対応

- **net/http・chi・gorilla/mux** — `func(http.Handler) http.Handler` なのでそのまま使用可
- **Echo** — `e.Use(echo.WrapMiddleware(mw))`。二重ラップでの捕捉・Flush 検知を [e2e/echoe2e](../e2e/echoe2e) で E2E 検証済み
- **Gin** — 専用アダプタを計画中（エクスポート部品 + コア `Begin`/`Finish` で構成）
- **fasthttp / Fiber** — 非対応

ルート単位の適用: `Require` はグローバル設定。特定ルートのみ保護したい場合はミドルウェアをそのルート/グループに適用する。

## 性能

Apple M1 Pro・`memstore`・`httptest` 込みの計測で、リプレイなし経路のオーバーヘッドは**約 +3.7μs / +36 allocs**（ベースライン 1.68μs → 5.33μs）。ネットワークストアでは Redis への往復が支配的。キーなしリクエストへの影響は約 +0.1μs。

## IETF ドラフトとの関係

ヘッダ名 `Idempotency-Key` と 400/409/422 の語彙は draft-ietf-httpapi-idempotency-key-header-07 と揃えている（業界の共通語彙のため）。同ドラフトは **RFC にならないまま 2026-04-18 に失効した** Internet-Draft であり、本ライブラリは「準拠」を謳わない。実クライアントと乖離する要求（RFC 8941 引用符付き文字列の必須化）は採用せず、生文字列を正規形とする。

## 設計資料

- [DESIGN.md](../DESIGN.md) — アーキテクチャと設計判断（英語）
- [REQUIREMENTS.md](REQUIREMENTS.md) — 実装契約（要件定義 v3.4）
- [ROADMAP.md](../ROADMAP.md) — マイルストーンと v1.1 計画

## ライセンス

[MIT](../LICENSE)
