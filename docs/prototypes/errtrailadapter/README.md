# errtrailadapter（検証用プロトタイプ）

契約 §9.1 / §12-8 の検証成果物。**v1 の公開 API には含まれない**（本実装は v1 リリース後に独立モジュールとして提供予定）。

目的: `httpidem` の公開インターフェースだけで errtrail 連携アダプタが書けることを、実物の [errtrail](https://github.com/repenguin22/errtrail) v1.3.2 に対して機械的に検証する。

- `Errors()` — httpidem の sentinel を errtrail カスタムコードにマッピングし、`problem.Write` で RFC 9457 応答を書く `ErrorWriter`
- `Policy()` — `httpidem.SetError` で通知されたエラーから `errtrail.CodeOf` で Code を取り出し、`Code.Retryable()`（レジストリ = テーブル駆動）で Persist/Discard を決める `ReplayPolicy`。非 errtrail エラーは `httpidem.DefaultPolicy`（ステータス駆動）へフォールバック

使用している公開 API: `httpidem.ErrorWriter` / `ReplayPolicyFunc` / `DefaultPolicy` / `SetError` / sentinel 6 種、`idemlease.Decision`。**内部 API への依存ゼロ** — このモジュールが CI でビルド・テストされ続ける限り、§12-13（アダプタ実装可能性）は機械的に担保される。
