> **リポジトリ注記**: 本文書は idemlease の**実装契約**（要件定義 v3.4）である。文書中の `idemtrail` は仮名称であり、本リポジトリでは `idemlease` に読み替える（パッケージ対応表は [ROADMAP.md](../ROADMAP.md) 冒頭を参照）。以下、原文は変更していない。
>
> **実装上の確定逸脱（errata）**: 原文 v3.4 から以下の 2 点を意図的に変更して実装している（v1.0.0 外部レビューを受けて確定。経緯は [docs/review/v1.0.0-external-review.md](review/v1.0.0-external-review.md)、設計理由は [DESIGN.md](../DESIGN.md)）。
>
> 1. **§6.1 の Flusher/Hijacker/Pusher 透過**: 原文は「元 writer が実装する場合のみ透過」。実装はラッパーが常にこれらのメソッドを持ち、`http.NewResponseController` 経由で委譲する（元 writer 非対応時は `http.ErrNotSupported` が返る）。二重ラップ環境（Echo 等）での実用性を優先した解釈（レビュー指摘 M6）
> 2. **§4.4 のデフォルト保存ヘッダ allowlist**: 原文の `Content-Type` / `Content-Language` / `Location` に **`Content-Encoding` / `Content-Disposition` を追加**（v1.0.1〜）。圧縮済みボディが `Content-Encoding` なしでリプレイされ破壊される欠陥の修正（レビュー指摘 H1）

---

# idemtrail — Idempotency-Key ライブラリ要件定義 v3.4（実装契約版）
HTTP API に冪等性保証を追加する、依存ゼロ（stdlib-only）のライブラリ。名称は仮。
v3.3 からの変更（コア API の穴 3 点を確定）:
1. `Finish` が `Options` を受け、Persist 時の RecordTTL は `o.RecordTTL` を `Complete` に渡す（A 案。httpidem が設定を透過しやすい）
2. reservation トークンの生成主体は **Begin**（crypto/rand、128bit 以上）。Store は保存・照合のみで生成・改変しない。適合性テストに保存忠実性を追加
3. `Proceed` 後に `Finish` を呼ばない場合の契約を明記: リース失効まで reserved（409 相当）、失効後に再実行可。コアは自動回収しない
v3.2 からの主な変更: **状態機械コアと HTTP 統合の分離**（§11 決定 12）。
1. `idemtrail`（コア）は HTTP 非依存の状態機械となり、キー・指紋・保存ペイロードを不透明な値として扱う。公開 API は `Begin` / `Finish`（§2.2）
2. `idemtrail/httpidem` が指紋アルゴリズム、ミドルウェア、キー文法、ErrorWriter、捕捉、KeyScope 等の HTTP 固有要素をすべて持つ。**利用者の既定の入口は httpidem のミドルウェア**（体験は v3.2 と同じ）
3. コアの語彙は Set/Verify のような 2 語ではなく Reserve/Complete/Release + Begin/Finish のまま維持する。in-flight 排他・token CAS・Release がインターフェースから消えると状態がミドルウェア側に漏れ戻るため
4. 分離は**同一 go.mod 内のパッケージ分割**で行う。net/http は stdlib であり、別モジュール化はコストだけ増える
**v1 スコープ**: core + httpidem + memstore + redistore + 適合性テスト + ベンチ + DESIGN/ROADMAP。
---
## 1. 目的・非目的・保証の言明
### 目的
- クライアントが `Idempotency-Key` ヘッダを付けて POST/PATCH を安全にリトライできるようにする
- **実運用で実証されたセマンティクス**（Stripe、Adyen、WorldPay 等の決済系 API が採用するパターン）を基準とする
- 素朴実装が踏む競合バグ（check-and-set 競合、処理中クラッシュ、レスポンス捕捉の罠）を正しく解決する
- 冪等性の判断ロジック（状態機械）を HTTP から独立させ、フレームワークアダプタ・将来の gRPC が同一コアを共有できるようにする
### 標準との関係（README に明記）
- IETF draft-ietf-httpapi-idempotency-key-header-07 を**参考文献**とする。ヘッダ名 `Idempotency-Key` と、キー欠落 400 / 処理中再送 409 / 確定後の異ペイロード 422 のステータス使い分けは、業界の共通語彙として draft-07 と揃える
- ただし draft-07 は RFC 未採択のまま失効（2026-04-18）した Internet-Draft であり、本ライブラリは「準拠」を謳わない。ドラフトの要求のうち実運用と乖離している部分（RFC 8941 引用符付き String の必須化）は採用しない
### 保証の言明（README 冒頭に掲載する契約）
> 本ライブラリは、**同一キー（スコープ内）に対して有効なリースを保持する実行が常に高々 1 つ**であることを保証する。exactly-once は保証しない。リース失効後の再実行や、Complete 失敗時のリトライにより、処理が複数回実行されることはありうる。より強い保証（業務トランザクションとの原子的合流）は v1.1 の pgstore で提供予定。
### 非目的
- exactly-once 実行の保証（上記の通り）
- 特定のエラーライブラリ・フレームワークへの依存（連携はすべてアダプタの責務）
- コアパッケージからの `net/http` への依存（§12-12 で機械的に検証）
- サービス間をまたぐ exactly-once 配信、クライアント側のキー生成・管理
- 利用者に常に Begin/Finish を書かせること（既定の体験はミドルウェア。コア直接利用はアダプタ実装者向け）
- v1 では: gRPC、ルート単位の Require 設定（グローバルのみ。ミドルウェアをルート単位で適用すれば実現可能とドキュメントに記載）、fasthttp 系（Fiber 等）
---
## 2. パッケージ構成とコア API
### 2.1 パッケージ構成
| パッケージ | go.mod | 依存 | 役割 |
|---|---|---|---|
| `idemtrail` | コア | stdlib（**net/http 不可**） | 状態機械（Begin/Finish）、Record、Store IF、Decision、sentinel |
| `idemtrail/httpidem` | コア同梱 | stdlib（net/http） | ミドルウェア、キー文法、指紋、StoredResponse、ErrorWriter、捕捉、KeyScope/KeyValidator、ReplayPolicy |
| `idemtrail/memstore` | コア同梱 | stdlib | 開発・テスト用 Store（単一プロセス限定と明記） |
| `idemtrail/idemtrailtest` | コア同梱 | stdlib | Store 適合性 + 状態機械テストスイート |
| `idemtrail/httpidem/httpidemtest` | コア同梱 | stdlib | HTTP シナリオ適合性テストスイート（リプレイ・409・422・障害注入・KeyScope 分離） |
| `idemtrail/redistore` | 別 | go-redis | Redis 実装 |
| `idemtrail/pgstore` | 別（v1.1） | database/sql + ドライバは利用側 | PostgreSQL 実装 + トランザクション合流（§9.3） |
責務の境界（何がどちらにあるか）:
| 要素 | 所属 |
|---|---|
| キー（不透明 string）、指紋（不透明 []byte）、保存ペイロード（不透明 []byte） | コア |
| Reserve/Complete/Release、リース、token CAS、期限、Decision（Persist/Discard） | コア |
| `Idempotency-Key` ヘッダのパース・文法、指紋の**計算方法**、HTTP ステータス、Retry-After、ヘッダ allowlist、RFC 9457、`Idempotency-Replayed`、KeyScope、KeyValidator、SetError、レスポンス捕捉 | httpidem |
### 2.2 コア API スケッチ
```go
package idemtrail
type Action int
const (
    Proceed Action = iota          // 初回。ハンドラ相当を実行し Finish を呼ぶこと
    Replay                          // 保存済み。Outcome.Payload を返却に使う
    RejectInFlight                  // 処理中。Outcome.RetryAfter を利用可能
    RejectFingerprintMismatch       // 確定済みレコードと指紋不一致
)
type Outcome struct {
    Action     Action
    Token      string        // Proceed のとき。Finish に渡す
    Payload    []byte        // Replay のとき。保存済みペイロード
    RetryAfter time.Duration // RejectInFlight のとき。残リース
}
type Decision int
const (
    Persist Decision = iota // Complete（保存）
    Discard                 // Release（破棄・再実行許可）
)
// Begin: キー・指紋を検査し、Reserve を含む前段の状態遷移を 1 回で行う。
// reservation トークンは Begin が crypto/rand で生成し（128bit 以上）、
// Record に載せて Reserve に渡す。Store はトークンを生成・改変しない。
func Begin(ctx context.Context, s Store, key string, fingerprint []byte, o Options) (Outcome, error)
// Finish: 後段の状態遷移。Persist なら payload とともに Complete
// （RecordTTL は o.RecordTTL を使用）、Discard なら Release。
// ErrTokenMismatch / ErrNotFound は「所有権喪失」として
// (LeaseLost=true, nil) に正規化して返す。
func Finish(ctx context.Context, s Store, key, token string, d Decision, payload []byte, o Options) (LeaseLost bool, err error)
```
**Proceed 後に Finish を呼ばない場合の契約（アダプタ実装者向け）**: レコードはリース失効まで reserved のまま残り、その間の同キーは `RejectInFlight`（409 相当）、失効後は再実行可能になる。これは呼び出し側のバグであり、コアは自動回収しない（リース失効が唯一の回収経路）。
- コアはキーの意味（スコープ済みか否か）、指紋の作り方、ペイロードの中身（HTTP レスポンスか gRPC メッセージか）を**一切解釈しない**
- Begin のエラーは Store 障害のみ（fail-open/closed の解釈は呼び出し側 = httpidem の責務）
- httpidem のミドルウェアは Begin/Finish の薄い皮であること（§12-13）
### 2.3 アダプタ戦略
| 系統 | 接続先 | 例 |
|---|---|---|
| Store アダプタ | `Store`（§3.2） | redistore、pgstore（v1.1） |
| フレームワーク/プロトコルアダプタ | コア Begin/Finish + httpidem の公開部品（指紋関数・Recorder） | ginadapter、gRPC（将来） |
| エラー処理アダプタ | `httpidem.ErrorWriter` / `httpidem.ReplayPolicy` | errtrailadapter（将来） |
フレームワーク対応方針: net/http / chi / gorilla/mux は httpidem ミドルウェアがそのまま動作。Echo は `echo.WrapMiddleware` で組み込み（専用アダプタなし、E2E テストで担保 §10）。Gin は専用アダプタ（将来、§9.2）で、コア Begin/Finish を直接使う。Fiber は非対応。
分けすぎの禁止（設計ガードレール）: httpidem を別 go.mod にしない（stdlib のみ）。Payload を過度に抽象化しない（v1 の保存内容は HTTP レスポンスであり、`StoredResponse` とそのシリアライズは httpidem が持つ）。利用者の既定 API はミドルウェアであり、Begin/Finish はアダプタ実装者向けと位置づける。
---
## 3. コアのドメインモデル（パッケージ `idemtrail`）
### 3.1 状態機械とレコード
```
(none) --Reserve--> reserved --Complete--> completed
                       |
                       +--(lease 失効 / Release)--> (none)
```
レコード内容: キー（不透明 string、最大長はコアでは制限しない）、指紋（[]byte）、状態、リース期限、レコード期限、reservation トークン（**Begin が crypto/rand で生成する 128bit 以上のランダム値**。Store は保存のみで生成・改変しない）、completed 時はペイロード（[]byte）。
期限切れの扱い（論理状態）: `reserved` かつリース期限超過 → 論理的に `(none)`。`completed` かつレコード期限超過 → 論理的に `(none)`。
Begin の判定表（§2.2 の Action に対応）:
| 既存レコード | 判定 |
|---|---|
| なし / 期限切れのみ | Reserve して `Proceed` |
| 有効な reserved（指紋問わず） | `RejectInFlight` + 残リース |
| 有効な completed・指紋一致 | `Replay` + ペイロード |
| 有効な completed・指紋不一致 | `RejectFingerprintMismatch` |
### 3.2 Store インターフェースと操作別セマンティクス
```go
type Store interface {
    // Reserve: キーを reserved 状態で原子的に確保する。
    // 有効な既存レコード（未期限切れ）があれば (existing, ErrAlreadyExists) を返す。
    // 期限切れレコードのみがある場合は存在しないものとして原子的に確保
    // しなければならない（上書き Reserve）。期限切れレコードを existing として
    // 返してはならない。GET→SET の 2 段階実装は不可。
    Reserve(ctx context.Context, rec Record) (existing *Record, err error)
    // Complete: reserved → completed。token 不一致は ErrTokenMismatch。
    // 対象レコードが無い（期限切れ削除済み等）場合は ErrNotFound。
    Complete(ctx context.Context, key, token string, payload []byte, recordTTL time.Duration) error
    // Release: reserved レコードを削除。token 不一致は ErrTokenMismatch。
    // 対象が無い場合は ErrNotFound。
    Release(ctx context.Context, key, token string) error
    // Get: 運用・デバッグ・アダプタ・適合性テスト用。リクエスト経路では
    // 使用しない（Reserve の existing で足りる）。期限切れは nil, nil でよい。
    Get(ctx context.Context, key string) (*Record, error)
}
```
- Reserve は SETNX / `INSERT ... ON CONFLICT` 等の単一原子操作で実装すること
- Complete / Release はトークン compare-and-set であること
- Store はトークンを生成・改変せず、Reserve で渡された `Record.Token` をそのまま保存・照合すること（生成主体は Begin。§2.2）
- sentinel: `ErrAlreadyExists`, `ErrTokenMismatch`, `ErrNotFound` をコアが定義し、全実装が `errors.Is` 互換で返すこと
- エラー使い分け（確定）: レコードは残っているがトークン不一致 → `ErrTokenMismatch`。レコードが無い → `ErrNotFound`
受け入れ条件: `idemtrailtest.RunStoreTests` が競合 Reserve（並行 100 本で成功 1）、リース失効後の再 Reserve、レコード TTL 失効後の再 Reserve、失効後の旧トークン Complete/Release の拒否（`ErrTokenMismatch` または `ErrNotFound`）、Reserve で渡したトークンが Get / existing でそのまま観測できること（保存の忠実性）を検証し、memstore / redistore が全件通ること。加えて Begin/Finish の状態遷移テーブル（§3.1）と「Proceed 後に Finish を呼ばない場合はリース失効まで RejectInFlight が続くこと」（§2.2 の契約）を fake Store 上で網羅するテストがあること。
### 3.3 リース失効 × 生存実行（確定）
リース失効後に別プロセスが同キーを Reserve した状態で、元の実行が完走した場合:
- 元実行側の Finish は Store から `ErrTokenMismatch`（または `ErrNotFound`）を受け、`LeaseLost=true` に正規化して返す
- 呼び出し側（httpidem）は捕捉済みレスポンスを**そのままクライアントに返す**（仕事は完了している）が、保存されないため**リプレイ対象にならない**。警告ログ（`idempotency_key`, 事象 = lease_lost）
- 新実行側の結果のみが completed として保存される
受け入れ条件: リース失効を強制するテストで上記のクライアント観測と保存状態を検証。
---
## 4. HTTP 統合（パッケージ `idemtrail/httpidem`）
ミドルウェアは `func(http.Handler) http.Handler`。対象メソッド: デフォルト POST / PATCH（設定可能）。
### 4.1 キー文法（確定・実ユースケース基準）
**正規形は非引用の生文字列**。draft-07 の RFC 8941 String（引用符付き）は互換受理し、デコード後の内側文字列を同一キーとして扱う。
受理規則（ヘッダ値を前後 OWS trim したあと）:
1. **生文字列（正規形）**: 先頭が `"` でない場合、trim 後の値をそのままキーとする。許容: **制御文字（0x00–0x1F, 0x7F）以外の任意のバイト列**。内部の空白・非 ASCII（UTF-8 含む）も許容
2. **RFC 8941 String（互換）**: 先頭・末尾が `"` で RFC 8941 §3.3.3 としてパース成功 → デコード後の文字列をキーとする。`"abc"` と `abc` は同一キー
3. 不正（400）: 制御文字を含む生文字列、`"` 開始でパース失敗、空
4. 長さ: 1〜255 バイト。0 または 256 以上は 400
5. ヘッダが複数ある場合は 400
既知のトレードオフ（明記）: 先頭が `"` の生キーは引用形式の開始と解釈されるため、RFC 8941 形式でエスケープが必要。
**`KeyValidator(func(string) bool)`**: 受理後の追加検証を差し込める（例: UUID 限定）。デフォルトなし。
受け入れ条件: 生 UUID、引用符付き UUID、`"abc"`/`abc` 同一性、内部空白、UTF-8 キー、制御文字（0x1F, 0x7F）拒否、不正エスケープ/未閉じ引用符、0/1/255/256 バイト、複数ヘッダ、前後 OWS、KeyValidator 拒否のテーブルテスト。
### 4.2 リクエスト指紋（確定）
```
SHA-256( strings.ToUpper(method) + "\n" + URL.EscapedPath() + "?" + URL.RawQuery + "\n" + body全バイト )
```
- クエリは含める。RawQuery が空でも `?` は常に置く。Host / ヘッダ / Content-Type は含めない。空 body はプレフィックスのみのハッシュ
- 指紋関数は**エクスポート**する（Gin アダプタが再利用。§2.3）
**body 読み取り（機能要件）**: ハンドラ実行前に `MaxRequestBody`（デフォルト 1 MiB）まで全読みし、`r.Body` を読み取り済みバイト列の NopCloser で復元して渡す。超過時: デフォルト 413 拒否。バイパス設定時は警告ログのうえキーの有無に関わらず冪等処理せず素通し（指紋も Begin もしない）。ハンドラには**既読バイト列と未読の残りを連結したもの**（`io.MultiReader` 相当）を渡す。
受け入れ条件: (a) 同一リクエスト 2 連投で指紋一致、(b) クエリ/body/メソッドのみ違いで不一致、(c) ハンドラが body 全量を読める（バイパス経路含む）、(d) 413/バイパス両モードのテスト。
### 4.3 挙動表と優先順位
判定の優先順位: 対象メソッド外 → 素通し。キーなし → `Require` に従う。文法違反・KeyValidator 拒否 → 400。`MaxRequestBody` 超過 → 413 or バイパス。以降はコア Begin の判定（§3.1）を HTTP に写像:
| コア Action / 状況 | 挙動 | HTTP |
|---|---|---|
| キーなし・`Require(true)` | 拒否 | 400 |
| キーなし・`Require(false)` | 素通し | — |
| 文法違反 | 拒否 | 400 |
| `Proceed` | ハンドラ → ReplayPolicy → Finish | — |
| `Replay` | 保存レスポンス + `Idempotency-Replayed: true` | 保存値 |
| `RejectInFlight`（指紋問わず） | 拒否 + `Retry-After` | 409 |
| `RejectFingerprintMismatch` | 拒否 | 422 |
| Begin が Store 障害 | §4.6 | 503（closed 時） |
参考: 400/409/422 の使い分けは draft-07 §2.7 と一致（共通語彙として維持）。
### 4.4 応答の細目
- **Retry-After**: `Outcome.RetryAfter` の切り上げ秒、最小 1
- **保存ヘッダ**: デフォルト allowlist `Content-Type`, `Content-Language`, `Location`。追加可能だが常時除外は上書き不可: `Set-Cookie`, `Authorization`, hop-by-hop（`Connection`, `Transfer-Encoding`, `Keep-Alive`, `Upgrade`, `Trailer`, `TE`, `Proxy-Authenticate`, `Proxy-Authorization`）。理由（セッション漏洩防止）を明記。受け入れ条件: `Set-Cookie` を追加してもリプレイ応答に含まれないテスト
- **StoredResponse**: ステータス + allowlist ヘッダ + ボディ。httpidem が定義し、コアのペイロード []byte へのシリアライズ/デシリアライズ（バージョンバイト付き）も httpidem の責務
- **リプレイ表示**: `Idempotency-Replayed: true`（Stripe 慣行。デフォルト on）
- **拒否応答**: RFC 9457 形式の最小 JSON（stdlib のみ）。sentinel（`ErrInFlight`, `ErrFingerprintMismatch`, `ErrKeyMissing`, `ErrKeyInvalid`）を `errors.Is` で判別可能。差し替え口 `ErrorWriter`:
```go
type ErrorWriter interface {
    WriteError(w http.ResponseWriter, r *http.Request, status int, err error)
}
```
### 4.5 KeyScope（セキュリティ）
キーがグローバルだと、クライアント A が B と同じキーを送った場合に **B の保存済みレスポンスが A にリプレイされる**漏洩と、409/422 による他者キーの使用状況観測が可能になる（draft-07 §5 の複合キー推奨に相当する問題）。
```go
httpidem.KeyScope(func(r *http.Request) string { ... })
```
- 戻り値（認証済みテナント ID 等）を格納キーの接頭辞として `scope + "\x00" + key` で連結（`\x00` は §4.1 で拒否されるため生キーと衝突しない）。スコープ合成は httpidem の責務で、コアは合成後のキーを不透明に扱う
- デフォルトはスコープなし（グローバル）。空文字返却もグローバル扱い。マルチテナント公開 API では設定を**強く推奨**と README のセキュリティ節に明記
受け入れ条件: 異なるスコープの同一キー・同一指紋が独立に初回処理され、同一スコープでは通常のリプレイ/409/422 になるテスト。
### 4.6 Store 障害時の挙動（操作別・確定）
| 局面 | デフォルト | 補足 |
|---|---|---|
| Begin（Reserve）障害 | **fail-closed**: 503 | `FailOpen(true)` で「冪等性なしで素通し + 警告ログ」。FailOpen はここにのみ作用 |
| Finish（Complete）障害 | 捕捉済みレスポンスはクライアントに返す + ベストエフォート Release + 警告ログ | Release も失敗ならリース失効まで reserved（再送は 409、失効後に再実行）。**冪等性が一時的に弱まる**ことを README に明記 |
| Finish（Release）障害 | 警告ログのみ | リース失効で自然解消 |
`LeaseLost=true`（§3.3）は障害ではなく所有権喪失として扱い、レスポンスは返すが保存されない。
受け入れ条件: 障害注入 fake Store で 3 局面のクライアント観測（ステータス・内容・後続再送）を固定するテスト。
---
## 5. リプレイポリシー（httpidem）
### 5.1 デフォルト（ステータス駆動・確定）
| 結果 | Decision |
|---|---|
| 2xx / 3xx | Persist |
| 4xx（429 除く） | Persist（再送に同じエラーを返す） |
| **429** | **Discard**（保存すると制限解除後も 429 が永遠にリプレイされる） |
| 5xx | Discard |
| panic | Discard + 再 panic（recover は上位の責務） |
ハンドラが何も書かなかった場合はステータス **200** として判定（net/http の暗黙 200 に合わせる）。
### 5.2 判定インターフェース
```go
type ReplayPolicy interface {
    // err はハンドラが httpidem.SetError(ctx, err) で通知した場合のみ非 nil。
    // 中身は解釈しない（将来のアダプタ用の通路）。
    Decide(status int, err error) idemtrail.Decision
}
```
`Decision` 型はコアの語彙（§2.2）を共有する。通知がなければステータスのみで判定。
### 5.3 「Discard」の意味
Release は「副作用が起きていない」ことを保証しない（§1 の保証の言明）。根本解決は v1.1 のトランザクション合流（§9.3）。
---
## 6. レスポンス捕捉（httpidem）
### 6.1 捕捉
- `http.ResponseWriter` をラップし、ステータス・allowlist ヘッダ・ボディを捕捉
- `Flusher` / `Hijacker` / `Pusher` は元 writer が実装する場合のみ透過（`http.NewResponseController` を検討）
- `Flush` / `Hijack` 時点で捕捉を放棄し Discard 扱い（ストリーミングは保存対象外）
- 保存上限 `MaxResponseBody`（デフォルト 1 MiB）。超過は Persist せず Discard + 警告ログ
- ログは slog。常に `idempotency_key` 属性（KeyScope 使用時はスコープも）。キーに秘密が含まれる場合を考慮し、設定でキーのハッシュ化出力を可能に
### 6.2 部品のエクスポート（アダプタのための必須要件）
- 捕捉ロジック（バッファ、ステータス記録、上限判定、Flush/Hijack 検知）を writer 非依存の `Recorder` としてエクスポートし、httpidem の writer ラッパーはその薄い皮とする
- 指紋関数（§4.2）、キーパース関数（§4.1）、StoredResponse のシリアライズもエクスポートする
- **前段・後段フローの公開はコア Begin/Finish がその役割を担う**（v3.2 §6.2 の要件は分離により自然に満たされる）。Gin アダプタの構成は「キーパース + 指紋 + Begin + Recorder(gin.ResponseWriter 版) + ReplayPolicy + Finish」となる
受け入れ条件: httpidem のミドルウェア本体が、コア Begin/Finish とエクスポートされた部品（キーパース・指紋・Recorder・シリアライズ）のみで組み立てられていること（§12-13）。
---
## 7. 設定 API スケッチ
```go
mw := httpidem.New(store,
    httpidem.Require(true),
    httpidem.LeaseTTL(30*time.Second),
    httpidem.RecordTTL(24*time.Hour),
    httpidem.MaxRequestBody(1<<20),
    httpidem.MaxResponseBody(1<<20),
    httpidem.Policy(myPolicy),
    httpidem.Errors(myErrorWriter),
    httpidem.FailOpen(false),
    httpidem.Methods("POST", "PATCH"),
    httpidem.StoreHeaders("Location", "Content-Type"),
    httpidem.KeyScope(scopeFromAuth),
    httpidem.KeyValidator(isUUID),
)
handler := mw(mux)
```
LeaseTTL / RecordTTL はコアの `idemtrail.Options` に透過的に渡る（httpidem は同一の Options を Begin と Finish の両方に渡す）。
---
## 8.（欠番）
---
## 9. 将来拡張
### 9.1 errtrail アダプタ（`errtrailadapter`、別 go.mod）
1. **ErrorWriter**: sentinel を errtrail カスタムコードにマッピングし `problem.Write` で応答
2. **ReplayPolicy**: `SetError` 通知から errtrail.Code を取り出しテーブル駆動で `Decision` を返す。非 errtrail エラーはステータス駆動へフォールバック
v1 リリース前にプロトタイプを書き、公開インターフェースだけで実装可能なことを検証する。
### 9.2 フレームワークアダプタ
- `ginadapter`（別 go.mod）: コア Begin/Finish + httpidem の公開部品で構成（§6.2）。優先度高（ROADMAP 明記）
- Echo は `echo.WrapMiddleware` で足りる想定。組み込み例をドキュメント掲載
- 他フレームワークはコミュニティ実装可能（コア API + 適合性テストが前提）
### 9.3 トランザクション合流 + pgstore（**v1.1**）
v1 コアには `TxStore` / `CompleteTx` 型を置かない。方針は v3.2 から変更なし: 将来 `CompleteTx(ctx, tx *sql.Tx, …)` 相当を提供、ハンドラは StoredResponse を自前構築して同一内容を書くヘルパー経由で応答、rollback はリース失効経路に合流。**実装前に成功/失敗マトリクスを受け入れ条件付きで完成させること**。
### 9.4 gRPC（分離の恩恵）
コアが HTTP 非依存になったため、gRPC interceptor は「metadata からキー取得 + リクエストメッセージのバイト列で指紋 + Begin/Finish + レスポンスメッセージをペイロードにシリアライズ」で構成でき、Store と状態機械を完全共有できる。metadata キー名の定義が必要。別 go.mod。
### 9.5 その他
ORM 向けトランザクション合流アダプタ、ペイロード圧縮。
---
## 10. 品質要件
- コア依存ゼロ（stdlib のみ、net/http 不可）。Go バージョンは使用 stdlib API から決定
- **並行テストの定義**: goroutine 100 本で同一キーを同時に叩き、リース有効期間内の実行が正確に 1 回であることを `-race` で検証。リース失効をまたぐ再実行は仕様（§1、§3.3 で挙動固定）
- `idemtrailtest`（Store 適合性 + Begin/Finish 状態遷移）と `httpidemtest`（HTTP シナリオ: リプレイ・409・422・障害注入・KeyScope 分離・in-flight 異指紋 409）を公開
- Echo E2E（`echo.WrapMiddleware` 経由、二重ラップでの捕捉・Flush 検知）。**優先度は中**: リリース判定のブロッカーにしない
- ベンチマーク: リプレイなし経路のオーバーヘッドを計測し README に掲載
- DESIGN.md / ROADMAP.md（ROADMAP に v1.1: pgstore + tx 合流、ginadapter、errtrailadapter、gRPC を記載）
---
## 11. 決定事項
| # | 決定 | 理由 |
|---|---|---|
| 1 | 独立リポジトリ。アダプタ配置は v1 後に判断 | errtrail 非依存方針と一致 |
| 2 | `SetError` を v1 に含める（`Decide(status, err)` 固定） | 後からの変更は破壊的。コストは小さい |
| 3 | 429 はデフォルト Discard | 制限解除後も 429 が永遠にリプレイされるのを防ぐ |
| 4 | ~~ドラフト固定~~ → 決定 9 に置換 | — |
| 5 | pgstore + tx 合流は v1.1。`TxStore` は v1 に置かない | 未実装 API を公開しない |
| 6 | ~~RFC 8941 正規形~~ → 決定 10 に置換 | — |
| 7 | 有効 reserved 中は異指紋でも 409。422 は completed の指紋不一致のみ | in-flight は「待て」、確定後は「直せ」 |
| 8 | 期限切れレコードは Reserve 時に未存在扱い（原子的上書きを MUST） | Store 実装の分岐を防ぐ。適合性テストと整合 |
| 9 | draft-07 は参考文献（ヘッダ名と 400/409/422 の語彙のみ）。「準拠」を謳わず、失効済み（2026-04-18）の事実を README に記載 | 期限切れドラフトに看板を依存しない |
| 10 | キー正規形は非引用の生文字列。制御文字以外の任意バイト列 1〜255 バイトを受理。RFC 8941 String は互換受理。`KeyValidator` で任意に制限可 | 実クライアントは生文字列を送る |
| 11 | KeyScope を httpidem に追加（デフォルトはグローバル）。マルチテナント公開 API では強く推奨 | 他クライアントのレスポンス漏洩・キー観測を防ぐ |
| 12 | **状態機械コア（idemtrail）と HTTP 統合（httpidem）を分離**。同一 go.mod 内のパッケージ分割。コアは net/http を import しない。コア API は Begin/Finish + Reserve/Complete/Release の語彙を維持（Set/Verify に縮めない）。**利用者の既定入口は httpidem ミドルウェア** | in-flight 排他・token CAS・Release が IF から消えると状態が境界側に漏れ戻る。Gin/gRPC がコアを共有できる。別 go.mod は stdlib のみのためコスト過剰 |
---
## 12. 受け入れ条件サマリ（実装完了の定義）
1. 指紋: §4.2 の 4 条件のテストが全て通る
2. キー文法: §4.1 のテーブルテストが全て通る
3. リース失効 × 生存実行: §3.3 のテストで挙動が固定されている
4. Store 障害: §4.6 の 3 局面のクライアント観測がテストで固定されている
5. ヘッダ: §4.4 の常時除外がテストで担保されている
6. 並行性: §10 の「リース有効期間内に実行 1 回」テストが `-race` で通る
7. 適合性: memstore / redistore が Store 適合性テスト全件（期限切れ再 Reserve 含む）を通る
8. errtrailadapter のプロトタイプが公開 API のみで書けることを確認済み
9. README に保証の言明、Complete 失敗時の弱化、Discard ≠ 副作用なし、draft-07 の位置づけ、KeyScope 推奨が明記されている
10. in-flight 異指紋 409 / completed 異指紋 422 のテストがあること
11. KeyScope: スコープ違いの同一キーが独立処理されるテストがあること
12. **コアパッケージが net/http を import していないことを CI で機械的に検証**（`go list -deps` 等）
13. **httpidem ミドルウェアがコア Begin/Finish + エクスポート部品のみで構成されている**（§6.2。コードレビュー基準 + アダプタ実装可能性の担保）
14. コア Begin/Finish の状態遷移テーブル（§3.1）を網羅するテストがあること
