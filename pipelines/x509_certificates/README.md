# X.509 Client Subject DN parser

This pipeline parses the `tls.client.X509.subject.distinguished_name` string into its individual Elastic Common Schema (ECS) subject fields.

It enriches the `tls.client.x509.subject` object by breaking the DN down into its components such as Common Name (CN), Organization (O) and an array of Organizational Units (OU) if multiple exist.

## Input

The pipeline parses the `tls.client.x509.subject.distinguished_name` field containing the DN.

**Example input document**:
```json
{
  "tls": {
    "client": {
      "x509": {
        "subject": {
          "distinguished_name": "CN=prod.user,OU=Application Support,OU=IT Department,O=Example Corp, C=US"
        }
      }
    }
  }
}
```

## Output

The pipeline adds new, parsed component fields alongside the original field.

**Example output document:**
```json
{
  "tls": {
    "client": {
      "x509": {
        "subject": {
          "distinguished_name": "CN=prod.user,OU=Application Support,OU=IT Department,O=Example Corp,C=US",
          "common_name": [
            "prod.user"
          ],
          "organizational_unit": [
            "Application Support",
            "IT Department"
          ],
          "organization": [
            "Example Corp"
          ],
          "country": [
            "US"
          ]
        }
      }
    }
  }
}

``` 

## Non-ECS fields used

This pipeline uses the following custom fields, these should be added to the mappings in your template.

|Field|Type|Description|
|-----|----|-----------|
|tls.client.x509.subject.email_address|keyword []|An array of email addresses parsed from the DN.|
