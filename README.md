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
   "sub_uuid": "6e6097bc-b23e-4f09-86a8-8a5a0fc06113",
   "files":[
      "f5e465ca-543e-44f5-9045-aebce6eb9ac3",
      "4320dc71-ac5f-4cb3-b1d3-d9a253a59e3b",
      ...
   ]
}
```


## Data archiving result

E.g. 1 
```
{
   "sub_uuid": "6e6097bc-b23e-4f09-86a8-8a5a0fc06113",
   "success": false,
   "error": "No sequence files in submission",
   "files":[]
}

```

E.g. 2
```
{
   "sub_uuid": "6e6097bc-b23e-4f09-86a8-8a5a0fc06113",
   "success": true,
   "error": null,
   "files":[
      {
         "uuid": "f5e465ca-543e-44f5-9045-aebce6eb9ac3",
         "file_name": "read1.fq",
         "cloud_url": "s3://{bucket_name}/{sub_uuid}/read1.fq",
         "size": 100,
         "compressed": true,
         "md5": "098f6bcd4621d373cade4e832627b4f6",
         "ena_upload_path": "{env}/{sub_uuid}/read1.fq.gz",
         "success": true,
         "error": null
      },
      {
         "uuid": "4320dc71-ac5f-4cb3-b1d3-d9a253a59e3b",
         "file_name": "read2.fq.gz",
         "cloud_url": "s3://{bucket_name}/{sub_uuid}/read2.fq.gz",
         "size": 100,
         "compressed": false,
         "md5": "098f6bcd4621d373cade4e832627b4f6",
         "ena_upload_path": "{env}/{sub_uuid}/read2.fq.gz",
         "success": true,
         "error": null
      },
      ...
   ]
}
```
## Development
### Requirements

Requirements for this project are listed in 2 files: `requirements.txt` and `dev-requirements.txt`.
The `dev-requirements.txt` file contains dependencies specific for development

The requirement files (`requirements.txt`, `dev-requirements.txt`) are generated using `pip-compile` from [pip-tools](https://github.com/jazzband/pip-tools) 
```
pip-compile requirements.in --output-file=- > requirements.txt
pip-compile dev-requirements.in --output-file=- > dev-requirements.txt
```
The direct dependencies are listed in `requirements.in`, `dev-requirements.in` input files.

### Install dependencies

* by using `pip-sync` from `pip-tools`
```
pip-sync requirements.txt dev-requirements.txt
```
* or by just using `pip install` 
```
    pip install -r requirements.txt
    pip install -r dev-requirements.txt
```


### Test
```
python -m unittest tests.e2e.test_archiver
```