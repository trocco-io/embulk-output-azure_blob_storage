# Azure Blob Storage file output plugin for Embulk
[![Build Status](https://travis-ci.org/embulk/embulk-output-azure_blob_storage.svg?branch=master)](https://travis-ci.org/embulk/embulk-output-azure_blob_storage)

[Embulk](http://www.embulk.org/) file output plugin stores files on [Microsoft Azure](https://azure.microsoft.com/) [Blob Storage](https://azure.microsoft.com/en-us/documentation/articles/storage-introduction/#blob-storage)


## Overview

* **Plugin type**: file output
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

First, create Azure [Storage Account](https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/).

- **account_name**: storage account name (string, required)
- **auth_method**: how to authenticate: `account_key`, `service_principal` or `sas_token` (string, default: `account_key`)
- **account_key**: primary access key (string, required when `auth_method` is `account_key`)
- **tenant_id**: directory (tenant) ID of the service principal (string, required when `auth_method` is `service_principal`)
- **client_id**: application (client) ID of the service principal (string, required when `auth_method` is `service_principal`)
- **client_secret**: client secret of the service principal (string, required when `auth_method` is `service_principal`)
- **sas_token**: SAS token, with or without the leading `?` (string, required when `auth_method` is `sas_token`)
- **container**: container name (string, required)
- **path_prefix**: prefix of target keys (string, required) (string, required)
- **file_ext**: e.g. "csv.gz, json.gz" (string, required)
- **blob_type**: `BLOCK_BLOB | PAGE_BLOB | APPEND_BLOB | UNSPECIFIED`, currently supports BLOCK_BLOB, other types will fall back into `UNSPECIFIED` (string, default `UNSPECIFIED`)


### Authentication

- `account_key`: the storage account's access key.
- `service_principal`: a Microsoft Entra ID application with a client secret. Give it a role that can write blobs, e.g. "Storage Blob Data Contributor", on the storage account or the container. The access token is refreshed automatically during long transfers.
- `sas_token`: an account SAS or a container SAS that allows writing (and creating) blobs (`w`, `c`; also `d` so that the uncommitted blocks of a failed `BLOCK_BLOB` upload can be deleted). A SAS has an expiry time: the transfer fails after it.

### Auto create container

container will create automatically when container doesn't exists.

With `service_principal` or `sas_token`, if the credentials cannot read the container's properties (a container SAS never can; nor can an account SAS without the container resource type or the read permission, or a role that only allows writing blobs), this check is skipped with a warning, and the upload fails if the container doesn't exist. If the container doesn't exist and the credentials cannot create it, the transfer fails at the start.
 
When a container was deleted, a container with same name cannot be created for at least 30 seconds.
It's a [service specification](https://technet.microsoft.com/en-us/library/dd179408.aspx#Anchor_3) of Azure Blob Storage.

## Example

```yaml
out:
  type: azure_blob_storage
  account_name: myaccount
  account_key: myaccount_key
  container: my-container
  path_prefix: logs/csv-
  file_ext: csv.gz
  blob_type: BLOCK_BLOB
  formatter:
    type: csv
    header_line: false
  encoders:
  - {type: gzip}
```

With a service principal or a SAS token:

```yaml
out:
  type: azure_blob_storage
  account_name: myaccount
  auth_method: service_principal
  tenant_id: 00000000-0000-0000-0000-000000000000
  client_id: 00000000-0000-0000-0000-000000000000
  client_secret: my-client-secret
  # or
  # auth_method: sas_token
  # sas_token: sv=2022-11-02&ss=b&srt=co&sp=rwdlc&se=2026-12-31T00:00:00Z&spr=https&sig=...
  container: my-container
  path_prefix: logs/csv-
  file_ext: csv
  formatter:
    type: csv
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```


## Test

```
$ ./gradlew test  # -t to watch change of files and rebuild continuously
```

To run unit tests, we need to configure the following environment variables.

When environment variables are not set, skip some test cases.

```
AZURE_ACCOUNT_NAME
AZURE_ACCOUNT_KEY
AZURE_CONTAINER
AZURE_CONTAINER_DIRECTORY (optional, if needed)
```

If you're using Mac OS X El Capitan and GUI Applications(IDE), like as follows.
```xml
$ vi ~/Library/LaunchAgents/environment.plist
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>my.startup</string>
  <key>ProgramArguments</key>
  <array>
    <string>sh</string>
    <string>-c</string>
    <string>
      launchctl setenv AZURE_ACCOUNT_NAME my-account-name
      launchctl setenv AZURE_ACCOUNT_KEY my-account-key
      launchctl setenv AZURE_CONTAINER my-container
      launchctl setenv AZURE_CONTAINER_DIRECTORY unittests
    </string>
  </array>
  <key>RunAtLoad</key>
  <true/>
</dict>
</plist>

$ launchctl load ~/Library/LaunchAgents/environment.plist
$ launchctl getenv AZURE_ACCOUNT_NAME //try to get value.

Then start your applications.
```
