## Unreleased (trocco)
* [new feature] Support `auth_method` option: `service_principal` (Microsoft Entra ID client credentials) and `sas_token`, in addition to `account_key`. Existing configurations keep using `account_key`.
* [maintenance] A failure to get an access token because of a wrong tenant ID, client ID or client secret is no longer retried.
* [maintenance] The final error of a failed upload is the last failure (not the first) and includes the HTTP status, the error code and Azure's error details.
* [maintenance] With `service_principal` or `sas_token`, the container check at the start is skipped with a warning when the credentials cannot read the container's properties (e.g. a container SAS).
* [maintenance] `BLOCK_BLOB`: the block list commit is retried like the blocks, and `abort` logs a warning instead of failing when it cannot delete the uncommitted blocks.

## 0.2.1 - 2022-03-30
* [maintenance] Remove deprecated functions [#21](https://github.com/embulk/embulk-output-azure_blob_storage/pull/21)

## 0.1.11 - 2022-12-09
* [maintenance] catch up with embulk 0.10.29 [#18](https://github.com/embulk/embulk-output-azure_blob_storage/pull/18)

## 0.1.10 - 2021-08-07
* [maintenance] Update the newest `azure-storage:8.6.6`
* [improvement] BLOCK_BLOB supports for large file upload, and code refactoring. [#20](https://github.com/embulk/embulk-output-azure_blob_storage/pull/20)

## 0.1.9 - 2019-12-09
* [maintenance] Fix issue of creating temporary file with nested path [#17](https://github.com/embulk/embulk-output-azure_blob_storage/pull/17)

## 0.1.8 - 2019-12-04
* [maintenance] Always delete temporary file [#15](https://github.com/embulk/embulk-output-azure_blob_storage/pull/15)

## 0.1.7 - 2018-10-19
* [maintenance] Close BufferedInputStream before delete temporary file [#14](https://github.com/sakama/embulk-output-azure_blob_storage/pull/14)

## 0.1.6 - 2018-10-18
* [maintenance] Fix bug that happens while deleting temporary files on Windows [#12](https://github.com/sakama/embulk-output-azure_blob_storage/pull/12)

## 0.1.5 - 2016-04-06

* [maintenance] Refactoring codes [#8](https://github.com/sakama/embulk-output-azure_blob_storage/pull/8)


## 0.1.4 - 2016-03-20

* [maintenance] Use RetryExecutor when retry is needed [#7](https://github.com/sakama/embulk-output-azure_blob_storage/pull/7)

## 0.1.3 - 2016-03-15

* [maintenance] Add retry logic using expotential backoff [#6](https://github.com/sakama/embulk-output-azure_blob_storage/pull/6)
* [maintenance] Change local file output path to temporary directory
[#5](https://github.com/sakama/embulk-output-azure_blob_storage/pull/5)

## 0.1.2 - 2016-02-02

* [maintenance] Upgrade embulk version to v0.8.2 [#3](https://github.com/sakama/embulk-output-azure_blob_storage/pull/3)

## 0.1.1 - 2015-11-16

* [maintenance] Added unit tests [#2](https://github.com/sakama/embulk-output-azure_blob_storage/pull/2)
* [maintenance] Refactored code [#1](https://github.com/sakama/embulk-output-azure_blob_storage/pull/1)
