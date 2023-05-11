# HOW TO RUN PARTNER RULE POC

## Obtain necessary certificates & node_descriptor

To match Partner Rule in provider, some signed files are needed.

Example ones are put here: [https://drive.google.com/drive/folders/1LpCrnsatthcD1w4BPVOYcV2IxZ1ty9_V](https://drive.google.com/drive/folders/1LpCrnsatthcD1w4BPVOYcV2IxZ1ty9_V)

Download following ones:

- `root-certificate.signed.json`
- `partner-certificate.signed.json`
- `partner-keypair.key`
- `node-descriptor.json`

Then edit `nodeId` of `node-descriptor.json` accordingly, and sign it with partner certificate with [golem-certificate-cli](https://github.com/golemfactory/golem-certificate) like so:

```bash
cargo run -p golem-certificate-cli sign node-descriptor.json partner-certificate.signed.json partner-keypair.key
```

Then, following files will be needed in next steps:

- `root-certificate.signed.json`
- `node-descriptor.signed.json`

## Set-up provider

Set up provider as always but with follwing rule set command:

```bash
cargo run  -p ya-provider -- rule set outbound partner import-cert root-certificate.signed.json --mode all
```

## Run task

Copy `node-descriptor.signed.json` to directory with `external_api_request.py` and run task:

```bash
poetry run python3 external_api_request.py
```

> **Note**
> Remember to set YAGNA_APPKEY and other env variables accordingly
