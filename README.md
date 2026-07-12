# idemlease

[![CI](https://github.com/repenguin22/idemlease/actions/workflows/ci.yml/badge.svg)](https://github.com/repenguin22/idemlease/actions/workflows/ci.yml)

`Idempotency-Key` ヘッダによる冪等性保証を HTTP API に追加する Go ライブラリ。
コア（状態機械）は依存ゼロ（stdlib のみ・`net/http` 非依存）で、HTTP 統合・ストア実装はパッケージとして分離される。

> **保証の言明（契約）**: 本ライブラリは、**同一キー（スコープ内）に対して有効なリースを保持する実行が常に高々 1 つ**であることを保証する。exactly-once は保証しない。リース失効後の再実行や、Complete 失敗時のリトライにより、処理が複数回実行されることはありうる。より強い保証（業務トランザクションとの原子的合流）は v1.1 の pgstore で提供予定。

**Status: pre-release（v1.0 に向けて開発中・API 未固定）**

- 実装契約: [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md)（要件定義 v3.4。文書中の仮名称 `idemtrail` は `idemlease` に読み替える）
- 開発計画: [ROADMAP.md](ROADMAP.md)

正式な README（保証の言明の詳細、Store 障害時の冪等性弱化、draft-07 の位置づけ、KeyScope 推奨、ベンチマーク結果など、契約 §12-9 の必須記載事項）は ROADMAP の M7 で整備する。
