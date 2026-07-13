# pgstore + トランザクション合流 設計（成功/失敗マトリクス）

契約 §9.3 のゲート成果物。**このマトリクスが契約者レビューで確定するまで M11/M12 は実装しない。**

- ステータス: **確定（2026-07-13 契約者レビュー承認。§6 の未決事項は提案どおり採用）**
- 対象: `pgstore`（PostgreSQL Store）+ `CompleteTx`（業務トランザクション合流）+ httpidem 連携プロトコル

## 1. 目的と強化される保証

v1 の保証は「同一キー（スコープ内）に対して有効なリースを保持する実行が常に高々 1 つ」であり、**Discard は副作用の不在を意味しない**（§5.3）。ハンドラの業務書き込みと冪等レコードの Complete が別トランザクションであるため、「業務は書けたが Complete に失敗」「Complete したが業務は失敗」の窓が残る。

CompleteTx はこの窓を閉じる。業務書き込みと reserved→completed 遷移を**同一 DB トランザクション**で行うことで:

> **強化された保証（README 掲載予定の言明）**: pgstore を使い、業務書き込みと `CompleteTx` を同一トランザクションで実行する場合、**同一キー（スコープ内）の業務トランザクションのコミットは、completed レコードが有効な間、高々 1 回**である。リース失効をまたいだ生存実行が競合しても、レコード行の行ロックと token CAS が直列化するため、敗者のトランザクションは業務書き込みごとロールバックされる。exactly-once に至らない残余は「RecordTTL 失効後の再実行」と「同一 DB の外にある副作用（外部 API 呼び出し等）」のみ。

## 2. 前提とスキーマ

```sql
-- pgstore.Schema(table) が返す DDL（利用者が migration に組み込む）
CREATE TABLE IF NOT EXISTS idemlease_records (
    key               bytea       PRIMARY KEY,         -- 不透明バイト列（KeyScope が NUL を含めるため text 不可）
    state             smallint    NOT NULL,            -- 1 = reserved, 2 = completed
    token             text        NOT NULL,
    fingerprint       bytea       NOT NULL DEFAULT ''::bytea,
    payload           bytea,
    lease_expires_at  timestamptz NOT NULL,
    record_expires_at timestamptz,
    created_at        timestamptz NOT NULL DEFAULT now()
);
```

- **期限権威は DB クロック**（v1.0.1 レビュー M1 の教訓を適用）。期限の設定・比較はすべて SQL 内の `now()` で行い、Go 側へは**残余時間**（`lease_expires_at - now()`）で返してローカル時計で絶対時刻を再構成する。アプリノード間の時計ずれはリース意味論を壊せない
- PostgreSQL に TTL はないため、期限切れ行は (a) Reserve の上書き（原子的 upsert）、(b) `Sweep(ctx, limit)` ヘルパー（任意・cron 等から呼ぶ）で回収する。放置しても論理的には不在（すべての SQL が期限を WHERE で見る）
- Reserve は単文 upsert: `INSERT ... ON CONFLICT (key) DO UPDATE SET （全フィールド初期化） WHERE 既存行が期限切れ` + 生存行の読み取り。claim 自体は単一の原子文であり（§3.2 の GET→SET 禁止を満たす）、claim 失敗後の existing 読みが失効と競合した場合はコアの有界リトライ（v1.0.1 M2）が吸収する
- Complete / Release / CompleteTx の CAS は `WHERE key = $1 AND token = $2 AND state = 1 AND lease_expires_at > now()`。**リース失効済みの自トークン行にも作用しない**（既存の適合性スイート「失効後の旧トークン拒否」と整合）
- 分離レベルは READ COMMITTED（デフォルト）で十分: CAS UPDATE の行ロックが直列化し、ブロック解除後の WHERE 再評価で敗者は 0 行になる

## 3. API 案

```go
package pgstore

// DBTX は *sql.DB / *sql.Tx の共通部分（database/sql のみに依存。ドライバは利用側）
type DBTX interface {
    ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
    QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func New(db *sql.DB, opts ...Option) *Store   // idemlease.Store 実装
func Schema() string                          // 上記 DDL
func Table(name string) Option                // テーブル名変更（既定 idemlease_records）

// CompleteTx は呼び出し側トランザクション内で reserved→completed を CAS 遷移させる。
// 戻り値は idemlease.Finish と同じ語彙:
//   - (false, nil)               遷移成功。tx を commit すれば業務書き込みと不可分に確定する
//   - (true, nil)                リース喪失（失効 / 別実行が所有）。呼び出し側は tx を ROLLBACK しなければならない（MUST）
//   - (false, ErrAlreadyCompleted) 同一 tx 内での二重呼び出し等、既に自分で complete 済み（呼び出しバグの検出）
//   - (false, err)               インフラ障害。呼び出し側は tx を ROLLBACK すべき
func (s *Store) CompleteTx(ctx context.Context, tx DBTX, key, token string, payload []byte, recordTTL time.Duration) (leaseLost bool, err error)

func (s *Store) Sweep(ctx context.Context, limit int) (deleted int, err error)
```

```go
package httpidem // 連携プロトコル（追加エクスポート）

// Reservation は Proceed 時にミドルウェアがコンテキストへ載せる予約情報。
// ハンドラはこれを CompleteTx に渡す（Key は KeyScope 合成済みのストアキー）。
func ReservationFromContext(ctx context.Context) (rsv Reservation, ok bool)
type Reservation struct{ Key, Token string; Options idemlease.Options }

// WriteStored は StoredResponse を w に書く（リプレイと同一の書き出し経路）。
// ハンドラは CompleteTx に渡した payload と同一内容をこれで応答する。
func WriteStored(w http.ResponseWriter, sr StoredResponse) error

// MarkFinished はミドルウェアの Finish（Persist/Discard）をスキップさせる。
// commit 成功後に呼ぶ。呼び忘れは T7 の挙動（誤警告ログ 1 行、実害なし）。
func MarkFinished(ctx context.Context)
```

ハンドラの定型（ドキュメント掲載予定）:

```go
rsv, _ := httpidem.ReservationFromContext(r.Context())
tx, _ := db.BeginTx(ctx, nil)
defer tx.Rollback()

// ... 業務書き込み ...
sr := httpidem.StoredResponse{StatusCode: 201, Header: h, Body: body}
payload, _ := sr.MarshalBinary()

leaseLost, err := store.CompleteTx(ctx, tx, rsv.Key, rsv.Token, payload, rsv.Options.RecordTTL)
if err != nil { /* 500: tx は defer で rollback */ }
if leaseLost { /* 409: rollback（別実行が所有）*/ }
if err := tx.Commit(); err != nil { /* 500: 未コミット */ }

httpidem.MarkFinished(r.Context())
_ = httpidem.WriteStored(w, sr)
```

## 4. 成功/失敗マトリクス

各行が受け入れテスト 1 本以上に対応する。「レコード」= idemlease_records の最終状態、「業務」= 同一 tx 内の業務書き込みの最終状態。

| ID | シナリオ | tx の結末 | レコード | 業務 | クライアント観測 | 同一キー再送時 |
|----|---|---|---|---|---|---|
| **T1** | 正常系: 業務書き込み → CompleteTx 成功 → commit → MarkFinished → WriteStored | commit | completed + payload | **committed** | ハンドラの応答 | Replay（保存内容と同一） |
| **T2** | 業務エラー（制約違反等）で rollback。CompleteTx の成否は問わない | rollback | reserved のまま | なし | 5xx（ポリシー Discard → ミドルウェアが Release） | Release 成功なら即再実行。失敗ならリース失効まで 409 → 再実行 |
| **T3** | CompleteTx が CAS 0 行 = リース喪失（失効後に**別実行が再予約済み**） | **MUST rollback** | 新所有者のもの（無傷） | なし | 409 + Retry-After | 新所有者の完了後は Replay |
| **T4** | CompleteTx が CAS 0 行 = リース失効・無主（再予約なし） | **MUST rollback** | 論理的に不在 | なし | 409 | 即再実行可 |
| **T5** | commit **前**にクラッシュ / 接続断 | 暗黙 rollback | reserved（リース失効待ち） | なし | 接続断 | リース失効まで 409 → 再実行（**契約 §9.3「rollback はリース失効経路に合流」**） |
| **T6** | commit **後**・応答前にクラッシュ / クライアント切断 | committed | completed | **committed** | 接続断 | **Replay が保存済み応答を返す（本機能の核心価値）** |
| **T7** | commit 成功だが MarkFinished を呼び忘れ（ハンドラバグ） | committed | completed | committed | 応答は届く | Replay ✓。副作用はミドルウェアの Finish(Persist) が CAS 0 行 → lease_lost **誤警告ログ 1 行のみ**（挙動をテストで固定し godoc に明記） |
| **T8** | 同一 tx 内で CompleteTx を二重呼び出し（ハンドラバグ） | 呼び出し側次第 | 1 回目のみ有効 | - | - | 2 回目は `(false, ErrAlreadyCompleted)`（rows=0 後の分類 SELECT で自トークン completed を検出。leaseLost=true で誤 rollback させない） |
| **T9** | **並行競合**: 実行 A が短リースで停滞 → 失効 → 実行 B が再予約し完走 → A が CompleteTx | A は rollback / B は commit | B の completed | **B の 1 回のみ**（A の業務書き込みはロールバック） | A: 409 / B: 正常応答 | B の Replay |
| **T10** | RecordTTL 失効後の同一キー再実行 | 新 tx が commit | 新しい completed | **再度コミットされる（仕様**: 高々 1 回の保証は completed の有効期間内**）** | 正常応答 | 新しい Replay |

### 受け入れ条件（テスト計画）

- **T1〜T8, T10**: postgres コンテナ上の結合テスト。各行について (a) レコード状態を SQL で直接検証、(b) 業務テーブル（テスト用 `orders` 相当）の行数を検証、(c) クライアント観測（ステータス・ボディ）、(d) 再送時挙動、の 4 点を固定する
- **T5**: tx を握ったまま接続を破棄して再現。**T6**: commit 後に応答を書かずに return して再現
- **T9**: `-race` 下の並行テスト。短リース + ハンドラ停滞で本物のリース失効競合を再現し（httpidemtest の LeaseExpirySurvivor と同型）、**業務行がちょうど 1 行**であることを一意制約なしのテーブルで検証（CAS の直列化だけで防げることを示す）
- 加えて: pgstore 自体が既存 3 スイート（RunStoreTests / RunStateMachineTests / RunHTTPTests）全件を通ること（M11 の Exit）

## 5. 非目的・制約

- **同一 DB の外の副作用**（外部 API、メール送信等）は保証対象外。README で明示する
- PostgreSQL 以外の RDB、ORM 連携（§9.5、v1.2+）、分散トランザクションは扱わない
- CompleteTx を使わない従来フロー（ミドルウェアの自動 Persist）は pgstore でも従来どおり動く（tx 合流はオプトイン）
- コアパッケージには型を追加しない（`CompleteTx` は pgstore、連携部品は httpidem。§11 決定 5 を維持）

## 6. 未決事項（レビューで確定したい点）

| # | 項目 | 提案 |
|---|---|---|
| 1 | テスト用ドライバ | `jackc/pgx/v5` の stdlib アダプタ（pgstore 本体は database/sql のみ依存を維持） |
| 2 | CI | `postgres:16-alpine` サービスコンテナ、`PG_DSN` 未設定時スキップ（redistore と同方式） |
| 3 | `Reservation.Key` の中身 | KeyScope 合成済みのストアキーを渡す（ハンドラはスコープを意識しない） |
| 4 | Sweep の提供形態 | メソッドのみ（デーモンは持たない）。運用は cron / 起動時に利用者が呼ぶ |
| 5 | T3/T4 の応答ステータス | どちらも 409（区別しない。無主かどうかはクライアントに無意味） |
