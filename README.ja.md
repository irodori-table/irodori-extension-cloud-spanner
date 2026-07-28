<!-- i18n: language-switcher -->
[English](README.md) | [日本語](README.ja.md)

# Cloud Spanner コネクタ

Cloud Spanner用のネイティブIrodoriテーブルコネクタ拡張機能。

このクレートは、コネクタのメタデータ、ネイティブABIのエクスポート、およびIrodori拡張マーケットプレイスで使用されるドライバ実装をパッケージ化しています。

## コネクタ

- 拡張機能ID: `irodori.cloud-spanner`
- エンジンID: `cloudSpanner`
- ワイヤープロトコル: `cloudSpanner`
- デフォルトポート: `443`
- ネイティブABI: `irodori.connector.native.v1`
- ドライバ連携: `はい`
- マーケットプレイスの公開範囲: `公開`
- パッケージバージョン: `0.1.4`

このパッケージはコネクタのメタデータとネイティブドライバを直接使用し、デスクトップアダプタのソーススナップショットは必要ありません。

コネクタのメタデータは `connector.config.json` と `irodori.extension.json` に格納されています。
Rustクレートは `src/lib.rs` からネイティブABIをエクスポートし、`irodori-connector-abi` を共有JSON/バッファヘルパーとして使用し、コネクタの動作は `src/driver.rs` に保持しています。

## 接続メタデータ

- エンドポイントモード: `cloudResource`, `customEndpoint`, `connectionString`
- トランスポートモード: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS対応: `はい`
- TLS必須（デフォルト）: `はい`
- カスタムドライバオプション: `はい`

### エンドポイントフィールド

| フィールド | ラベル | 型 | 必須 |
| --- | --- | --- | --- |
| `projectId` | Google Cloud プロジェクト | `string` | はい |
| `instanceId` | Spannerインスタンス | `string` | はい |
| `databaseId` | Spannerデータベース | `string` | はい |
| `endpoint` | カスタムエンドポイント | `uri` | いいえ |

## 認証

コネクタはこれらの認証モードを公開しており、クライアントは適切な資格情報フィールドをレンダリングできます。必要に応じて、ドライバ固有またはプロバイダー固有の値も `options` を通じて渡すことが可能です。

| 認証方法 | ラベル | 種類 | シークレットの用途 |
| --- | --- | --- | --- |
| `none` | 認証なし | `none` | なし |
| `connectionString` | 接続文字列 / DSN | `connectionString` | なし |
| `oauthAccessToken` | OAuth 2.0アクセストークン | `token` | `token` |
| `serviceAccountJson` | サービスアカウントJSON | `serviceAccount` | `privateKey` |
| `serviceAccountJwt` | サービスアカウントJWT秘密鍵 | `privateKey` | `privateKey`, `privateKeyPassphrase` |
| `serviceAccountImpersonation` | サービスアカウントのなりすまし | `iam` | `token` |
| `googleApplicationDefaultCredentials` | アプリケーションデフォルト資格情報 | `iam` | なし |
| `oauth2` | OAuth 2.0 | `oauth2` | `token` |
| `workloadIdentity` | ワークロードアイデンティティ連携 | `iam` | `token` |
| `customDriverOptions` | カスタムドライバオプション | `custom` | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## ネイティブABI呼び出し

| メソッド | 応答 |
| --- | --- |
| `health` | コネクタのヘルス状態、エンジンID、ABIバージョン、ドライバの状態を返します。 |
| `describe` | 埋め込みマニフェストとコネクタ設定を返します。 |
| `manifest` | 生の `irodori.extension.json` を返します。 |
| `config` | 生の `connector.config.json` を返します。 |
| `connect` | ネイティブコネクタ接続を開き、検証します。 |
| `query` | コネクタクエリを実行し、構造化された行またはJSON結果を返します。 |
| `metadata` | スキーマ、テーブル、カラム、インデックス、コレクション、または同等のメタデータを読み取ります。 |
| `close` | キャッシュされたネイティブ接続を閉じて削除します。 |

## 開発

このチェックアウト内のすべての拡張クレートは `../target` を共有しており、依存関係は一度だけコンパイルされます。

```sh
make check
make build
```

リリースパッケージはプラットフォーム固有のネイティブアーティファクトを `dist/native` に配置します。

## ライセンス

0BSD。ほぼすべての目的でこのプロジェクトを使用、コピー、修正、配布できます。