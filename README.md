# Parsing-FHIR-Files
This repo contains input and output files, along with the code that has been explained in the following medium article:
[Parsing FHIR Files - AWS Glue](https://medium.com/@hamzaehsankhan/parsing-fhir-files-aws-glue-d861010af02c)

There are two different FHIR files that we have processed.

For *OpenEpic*, following steps were completed:
- Read test-patient.json, test-patient1.json, test-patient3.json files from a path in S3. Available [here](www.okay.com)
- Used a StructType schema, reading 'Identifiers and Credentials' as a JSON string for now, to later convert it into map type.
- Flattened the map type into individual columns.
- Converted ArrayType column Applicable Resources to string, as CSV does not support ArrayType columns
- Saved a CSV to S3.

For *FHIR* patient resouces, following steps were completed:
- Read patient-example.json, patient-example-a.json, patient-example-b.json, patient-example-c.json, and patient-example-d.json. Available [here](www.okay.com)
- Used a schema, with modifications for name, address, telecom fields. Read them as array of strings rather than array of structs.
- Extracted official_given_name from name field.
- Extracted home_address from address field.
- Extracted city from address field.
- Extracted phone from address field.
- Filename tagging for data lineage.
- Saved dataframe as CSV