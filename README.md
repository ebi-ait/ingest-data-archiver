# ingest-data-archiver

__Data archiving request__

- Mandatory field: `sub_uuid` - the submission uuid.
- Optional field: `files` - list of file names in submission. 
    - If unspecified, all files in the submission is archived.
    - This is useful to allow retry of individual data file(s) archiving in case of a failure.

E.g. 1 Without `files` property.
```
{
   "sub_uuid":"6e6097bc-b23e-4f09-86a8-8a5a0fc06113"
}
```

E.g. 2
```
{
   "sub_uuid":"6e6097bc-b23e-4f09-86a8-8a5a0fc06113",
   "files":[
      "read1.fq",
      "read2.fq"
   ]
}
```


__Data archiving result__
```
{
   "sub_uuid":"6e6097bc-b23e-4f09-86a8-8a5a0fc06113",
   "success":true,
   "error":"msg",
   "files":[
      {
         "file_name":"read1.fq",
         "md5":"098f6bcd4621d373cade4e832627b4f6",
         "success":true,
         "error":"msg"
      },
      {
         "file_name":"read2.fq",
         "md5":"ad0234829205b9033196ba818f7a872b",
         "success":true,
         "error":"msg"
      }
   ]
}
```
