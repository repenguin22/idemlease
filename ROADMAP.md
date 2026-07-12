# idemlease ROADMAP — v1.0

実装契約は [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md)（要件定義 v3.4）。本 ROADMAP と契約の記述が食い違う場合は**契約が優先**する。本文中の「§n」参照はすべて同文書のセクション番号を指す。

## 名称の読み替え

要件定義は仮名称 `idemtrail` を用いている。本リポジトリの正式名称は `idemlease` とし、以下のとおり読み替える。

| 要件定義上の名称 | 本リポジトリ | go.mod |
|---|---|---|
| `idemtrail`（コア） | `idemlease` | ルート |
| `idemtrail/httpidem` | `idemlease/httpidem` | ルート |
| `idemtrail/memstore` | `idemlease/memstore` | ルート |
| `idemtrail/idemtrailtest` | `idemlease/idemleasetest` | ルート |
| `idemtrail/httpidem/httpidemtest` | `idemlease/httpidem/httpidemtest` | ルート |
| `idemtrail/redistore` | `idemlease/redistore` | **別 go.mod** |
| `idemtrail/pgstore`（v1.1） | `idemlease/pgstore` | **別 go.mod** |

## v1.0 のスコープ（契約の要約）

- コア状態機械 `idemlease`（stdlib のみ・`net/http` 非依存。公開 API は `Begin` / `Finish`）
- HTTP 統合 `httpidem`（**利用者の既定の入口はミドルウェア**）
- `memstore`（開発・テスト用、単一プロセス限定）
- `redistore`（go-redis、別 go.mod）
- 適合性テストの公開: `idemleasetest`（Store 適合性 + 状態遷移）/ `httpidemtest`（HTTP シナリオ）
- ベンチマーク（リプレイなし経路のオーバーヘッド → README 掲載）
- DESIGN.md / ROADMAP.md / README（§12-9 の必須記載事項）

**やらないこと（v1）**: exactly-once 保証、pgstore・トランザクション合流（→ v1.1）、gRPC、ルート単位の Require 設定、fasthttp 系（Fiber）、特定エラーライブラリへの依存。

## 設計ガードレール（全マイルストーン共通）

- コアは `net/http` を import しない。**CI で機械的に検証**する（§12-12、`go list -deps`）
- `httpidem` を別 go.mod にしない（同一 go.mod 内のパッケージ分割。§11 決定 12）
- ミドルウェアはコア `Begin`/`Finish` + エクスポート部品のみで組み立てる薄い皮（§12-13）
- コアの語彙は Begin/Finish + Reserve/Complete/Release を維持し、Set/Verify に縮めない（§11 決定 12）
- reservation トークンの生成主体は **Begin**（crypto/rand、128bit 以上）。Store は保存・照合のみ（§2.2）
- Payload を過度に抽象化しない（`StoredResponse` とそのシリアライズは httpidem の責務。§2.3）
- **ランタイム依存ゼロを維持**。テストヘルパー（go-cmp / testify 等）は同一モジュール内の `_test.go` に限り使用可。公開適合性パッケージ（`idemleasetest` / `httpidemtest`）には持ち込まない（契約 §2.1 の「依存 = stdlib」を維持。Store 実装者に依存を強制しないため）。ビルド依存に漏れていないことは CI で機械検証

## マイルストーン

依存関係: `M0 → M1 → M2 → { M3 → M4 | M5 } → M6 → M7`（M5 は M2 完了後、M3/M4 と並行可）。

### M0 — リポジトリ基盤

- [x] `git init`（main ブランチ）/ `.gitignore` / `.gitattributes`
- [x] ROADMAP.md / README（暫定版）/ docs/REQUIREMENTS.md（契約 v3.4 の取り込み）
- [x] `go.mod` 作成（`github.com/repenguin22/idemlease`、go 1.22）+ コアパッケージの器（doc.go）
- [x] LICENSE（MIT）
- [x] GitHub リモート設定・初回 push（https://github.com/repenguin22/idemlease）
- [x] CI（GitHub Actions）: `gofmt` / `go vet` / `go test -race ./...`（Go 1.22.x + stable のマトリクス）
- [x] CI: **コアパッケージの `net/http` 非依存チェック**（§12-12。`go list -deps` の結果に `net/http` が含まれないことを機械検証）

**Exit criteria**: 空のコアパッケージで CI がグリーン。

### M1 — コア状態機械（`idemlease`）

**状態: ✅ 完了（2026-07-12）**

**成果物**
- 型定義: `Action` / `Outcome` / `Decision` / `Record` / `Options` / `Store` インターフェース / sentinel（`ErrAlreadyExists` `ErrTokenMismatch` `ErrNotFound`）（§2.2, §3.2）
- `Begin`: キー・指紋・ペイロードを不透明値として扱う。トークンを crypto/rand で生成（128bit 以上）して `Record` に載せ `Reserve` へ。判定表 §3.1（なし/期限切れ→`Proceed`、有効 reserved→`RejectInFlight`+残リース、completed 指紋一致→`Replay`、completed 指紋不一致→`RejectFingerprintMismatch`）
- `Finish`: `Persist`→`Complete`（RecordTTL は `o.RecordTTL`）/ `Discard`→`Release`。`ErrTokenMismatch`・`ErrNotFound` は `LeaseLost=true, nil` に正規化（§2.2, §3.3）
- テスト用 fake Store（障害注入・時刻制御が可能なもの。M2 で `idemleasetest` に昇格させる前提で設計）

**テスト（受け入れ）**
- Begin/Finish の状態遷移テーブル網羅（§12-14）
- Proceed 後に Finish を呼ばない場合の契約: リース失効まで `RejectInFlight`、失効後に再実行可（§2.2）
- リース失効 × 生存実行で `LeaseLost=true` 正規化（§3.3 のコア分）

**Exit criteria**: 上記テスト green + net/http 非依存チェック green。

### M2 — Store 適合性キット + memstore

**状態: ✅ 完了（2026-07-12）**

**成果物**
- `idemleasetest.RunStoreTests`（§3.2 受け入れ条件）:
  - 競合 Reserve（並行 100 本で成功 1）
  - リース失効後の再 Reserve / RecordTTL 失効後の再 Reserve（期限切れは**原子的上書き** Reserve。GET→SET の 2 段階実装を検出して落とす）
  - 失効後の旧トークンによる Complete/Release の拒否（`ErrTokenMismatch` または `ErrNotFound`）
  - **トークン保存忠実性**: Reserve で渡したトークンが Get / existing でそのまま観測できる
- Begin/Finish 状態遷移スイート（fake Store 上の網羅テスト）も公開側に含める
- `memstore`: 単一プロセス限定と明記。期限は論理判定（lazy expiration）

**Exit criteria**: memstore が適合性全件を `-race` で通過（§12-7 の memstore 分）。

### M3 — HTTP 統合（`httpidem`）

**状態: ✅ 完了（2026-07-12）**

**成果物**
- キー文法 §4.1: 生文字列が正規形（制御文字以外の任意バイト列 1〜255 バイト）、RFC 8941 String 互換受理（`"abc"` ≡ `abc`）、複数ヘッダ 400、`KeyValidator`
- 指紋 §4.2: `SHA-256(METHOD + "\n" + EscapedPath + "?" + RawQuery + "\n" + body)`。**関数をエクスポート**。body 全読み（`MaxRequestBody` 既定 1 MiB、超過は 413 or バイパス素通し）と `r.Body` 復元（MultiReader 相当）
- ミドルウェア（`func(http.Handler) http.Handler`、既定メソッド POST/PATCH）: 挙動表 §4.3 の写像 — 400 / 409(+`Retry-After` 切り上げ秒・最小 1) / 422 / リプレイ(+`Idempotency-Replayed: true`)
- `StoredResponse` + バージョンバイト付きシリアライズ、保存ヘッダ allowlist（既定 `Content-Type` `Content-Language` `Location`。`Set-Cookie`・hop-by-hop 等は上書き不可の常時除外）（§4.4）
- 拒否応答: RFC 9457 形式の最小 JSON + `ErrorWriter` 差し替え口 + sentinel（`ErrInFlight` `ErrFingerprintMismatch` `ErrKeyMissing` `ErrKeyInvalid`）
- `KeyScope`: `scope + "\x00" + key` 連結、既定グローバル（§4.5）
- Store 障害 §4.6: Begin 障害は fail-closed 503 既定（`FailOpen(true)` は Begin にのみ作用）/ Complete 障害は捕捉済みレスポンス返却 + ベストエフォート Release + 警告 / Release 障害は警告のみ
- `ReplayPolicy` §5: 既定はステータス駆動（2xx/3xx/4xx → Persist、**429**・5xx → Discard、panic → Discard + 再 panic、未書き込みは 200 扱い）。`SetError(ctx, err)` 通路
- `Recorder` §6: writer 非依存でエクスポート。Flush/Hijack 検知で捕捉放棄 → Discard、`MaxResponseBody`（既定 1 MiB）超過は Discard + 警告。slog ログ（`idempotency_key` 属性常時、キーのハッシュ化オプション）
- 設定 API §7 の option 一式

**Exit criteria**: §12-1（指紋 4 条件）・§12-2（キー文法テーブル）・§12-5（常時除外ヘッダ）・§12-13（Begin/Finish + 公開部品のみで構成）green。

### M4 — HTTP 適合性キット + 総合シナリオ（`httpidemtest`）

**状態: ✅ 完了（2026-07-12）**

**成果物**
- 公開スイート: リプレイ / 409（**in-flight 異指紋 409** 含む）/ 422（completed 異指紋）/ Store 障害 3 局面のクライアント観測固定 / KeyScope 分離（スコープ違いの同一キー・同一指紋が独立処理、同一スコープでは通常動作）
- リース失効 × 生存実行の HTTP 挙動固定: レスポンスは返すが保存されずリプレイ対象にならない、警告ログ（lease_lost）、新実行のみ completed 保存（§3.3）
- 並行テスト: goroutine 100 本 × 同一キーで「リース有効期間内の実行が正確に 1 回」を `-race` で検証(§10)

**Exit criteria**: §12-3・§12-4・§12-6・§12-10・§12-11 green。

### M5 — redistore（別 go.mod）

**状態: ✅ 完了（2026-07-13）**

**成果物**
- go-redis 実装: Reserve = SET NX（PX）による単一原子操作（期限切れの原子的上書き含む）、Complete/Release = Lua スクリプトによる token CAS
- トークンは保存・照合のみ（生成・改変しない — 適合性の保存忠実性テストで担保）
- CI: Redis を service container で起動し `idemleasetest` 全件 + `-race`
- マルチモジュール CI（ルート / redistore の 2 ジョブ）とタグ運用（`redistore/vX.Y.Z`）の整備

**Exit criteria**: §12-7 完了（memstore / redistore とも適合性全件通過）。

### M6 — 実証・性能

- **errtrailadapter プロトタイプ**: 公開インターフェース（`ErrorWriter` / `ReplayPolicy` / `SetError`）のみで実装可能なことを検証（§9.1, §12-8）。v1 にはマージしない（検証目的）
- **ベンチマーク**: リプレイなし経路（Begin → handler → Finish）のオーバーヘッドを計測し、README 掲載値を確定(§10)
- **Echo E2E**: `echo.WrapMiddleware` 経由の組み込み、二重ラップでの捕捉・Flush 検知(§10)。**優先度は中 — リリース判定のブロッカーにしない**

**Exit criteria**: §12-8 確認 + ベンチ数値取得。

### M7 — ドキュメント & v1.0.0 リリース

- README 正式版（§12-9 の必須記載）:
  - 保証の言明（冒頭）/ Complete 失敗時の冪等性の一時的弱化 / Discard ≠ 副作用なし / draft-07 の位置づけ（参考文献であり「準拠」を謳わない・2026-04-18 失効の事実）/ KeyScope 強推奨（セキュリティ節）
  - memstore の単一プロセス限定 / ベンチ結果 / フレームワーク対応方針（net/http・chi・gorilla はそのまま、Echo は WrapMiddleware、Gin は将来アダプタ、Fiber 非対応）/ ルート単位 Require の代替（ミドルウェアのルート単位適用）
- DESIGN.md: §11 決定事項の背景（特に決定 12: コア/HTTP 分離）、キー文法・指紋設計のトレードオフ、Store セマンティクス、リースモデル
- 使用例: net/http / chi / Echo 組み込み例
- リリース前チェック: **§12 の受け入れ条件 14 項目を全て確認** → `v1.0.0` タグ
- redistore のリリース手順: ルートを `v1.0.0` でタグ → redistore/go.mod の `replace` を外し `require github.com/repenguin22/idemlease v1.0.0` に更新 → `redistore/v1.0.0` をタグ

## 受け入れ条件（§12）↔ マイルストーン対応表

| §12 | 内容（要約） | マイルストーン |
|---|---|---|
| 1 | 指紋 §4.2 の 4 条件 | M3 |
| 2 | キー文法 §4.1 のテーブルテスト | M3 |
| 3 | リース失効 × 生存実行の挙動固定 | M1（コア正規化）/ M4（HTTP 観測） |
| 4 | Store 障害 3 局面のクライアント観測固定 | M4 |
| 5 | 常時除外ヘッダの担保 | M3 |
| 6 | 並行 `-race`「リース有効期間内に実行 1 回」 | M4 |
| 7 | memstore / redistore の適合性全件通過 | M2 / M5 |
| 8 | errtrailadapter プロトタイプの実装可能性確認 | M6 |
| 9 | README 必須記載事項 | M7 |
| 10 | in-flight 異指紋 409 / completed 異指紋 422 | M3–M4 |
| 11 | KeyScope 分離テスト | M3–M4 |
| 12 | コア net/http 非依存の CI 機械検証 | M0（整備）〜 M1 以降常時 |
| 13 | ミドルウェアが Begin/Finish + 公開部品のみで構成 | M3 |
| 14 | コア状態遷移テーブルの網羅テスト | M1 |

## v1.0 以降（v1.1+）

契約 §9・§11 決定 5 より:

- **v1.1 — pgstore + トランザクション合流**: `CompleteTx(ctx, tx *sql.Tx, …)` 相当を提供。**着手条件: 成功/失敗マトリクスを受け入れ条件付きで完成させてから実装**（§9.3）。`TxStore` / `CompleteTx` 型は v1 コアには置かない
- **ginadapter**（別 go.mod・優先度高）: コア Begin/Finish + httpidem 公開部品（キーパース・指紋・Recorder・シリアライズ）で構成（§9.2）
- **errtrailadapter 本実装**: sentinel → errtrail カスタムコードのマッピング ErrorWriter、`SetError` 通知のテーブル駆動 ReplayPolicy（§9.1）
- **gRPC interceptor**（別 go.mod）: metadata キー名の定義から着手。Store と状態機械はコアを完全共有（§9.4）
- その他: ペイロード圧縮、ORM 向けトランザクション合流アダプタ（§9.5）

## 決定・未決事項

| 項目 | 状態 | 内容 |
|---|---|---|
| LICENSE | 決定（2026-07-12） | MIT |
| モジュールパス | 決定（2026-07-12） | `github.com/repenguin22/idemlease` |
| Go 最低バージョン | 決定（2026-07-12） | **1.22**（go directive）。契約 §10 の下限（`log/slog` = 1.21）を満たす。CI は 1.22.x と stable の 2 系でテスト |
| テストヘルパー依存 | 決定（2026-07-12） | `_test.go` では go-cmp / testify 等を使用可（go-cmp v0.7.0 導入済み）。本体と公開適合性パッケージは stdlib のみを維持し、CI でビルド依存の stdlib-only を機械検証 |
| 公開 README の言語 | 未決 | 提案: 英語（日本語版を docs/ に併置） |
