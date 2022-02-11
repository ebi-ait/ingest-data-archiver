# ingest-data-archiver

Ingest component that archives (uploads) data files of a specified submission to the ENA upload area.

Refer to the Data Files Archiving document [here](https://github.com/ebi-ait/ingest-archiver/blob/dev/doc/data_files_archiving.md) for more info.

The request is normally triggered by another ingest component (for e.g. core or archiver) by sending a message to the `ingest.data.archiver.request.queue` in the format below. There is now an externally available endpoint exposed by the `ingest-archiver` to trigger data archiving.

See step 2 under __Uploading files directly to ENA__ of the [Archiving SOP](https://ebi-ait.github.io/hca-ebi-wrangler-central/SOPs/archiving_SOP.html).


## Data archiving request

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


## Data archiving result
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


## Test
```
python -m unittest tests.e2e.test_archiver
```