# Storage Profiles

Currently, we support the following storage profiles:

- S3 (tested with aws & minio)
- Azure Data Lake Storage Gen 2

## S3

We support remote signing and vended-credentials with minio & aws. Remote signing works for both out of the box, vended-credentials needs some additional setup for aws.

### AWS

To use vended-credentials with aws, your storage profile needs to contain

```json
{
  "warehouse-name": "test",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "examples",
    "key-prefix": "initial-warehouse",
    "assume-role-arn": null,
    "endpoint": null,
    "region": "local-01",
    "path-style-access": true,
    "sts_role_arn": "arn:aws:iam::....:role/....",
    "flavor": "aws"
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "...",
    "aws-secret-access-key": "..."
  }
}
```

The `sts_role_arn` is the role the temporary credentials are assume.

The storage-credential, i.e. `aws-access-key-id` & `aws-secret-access-key` should belong to a user which has `[s3:GetObject, s3:PutObject, s3:DeleteObject]` and the `[sts:AssumeRole]` permissions.

The `sts_role_arn` also needs a trust relationship to the user, e.g.:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::....:user/...."
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### Minio

For minio, the setup does not require any additional configuration, we use AssumeRole using the provided credentials in the storage profile to get temporary credentials. Any provided `sts_role_arn` is ignored.

```json
{
  "warehouse-name": "test",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "examples",
    "key-prefix": "initial-warehouse",
    "assume-role-arn": null,
    "endpoint": "http://localhost:9000",
    "region": "local-01",
    "path-style-access": true,
    "sts_role_arn": null,
    "flavor": "minio",
    "sts-enabled": false
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "minio-root-user",
    "aws-secret-access-key": "minio-root-password"
  }
}
```

## Azure Data Lake Storage Gen 2

For Azure Data Lake Storage Gen 2, the app registration your client credentials belong to needs to have the following permissions for the storage account (`account-name`):

- `Storage Blob Data Contributor`
- `Storage Blob Delegator`

A sample storage profile could look like this:

```json
{
  "warehouse-name": "test",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "azdls",
    "filesystem": "...",
    "key-prefix": "...",
    "account-name": "..."
  },
  "storage-credential": {
    "type": "az",
    "credential-type": "client-credentials",
    "client-id": "...",
    "client-secret": "...",
    "tenant-id": "..."
  }
}
```
