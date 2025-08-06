# Elasticsearch ingest pipeline snippets

A community-driven collection of reusable [Elasticsearch ingest pipelines](https://www.elastic.co/docs/manage-data/ingest/transform-enrich/ingest-pipelines) for data transformation tasks.

An ingest pipeline is a series of processors that run in sequence to modify documents before they are indexed. This allows you to pre-process data without needing external tools like Logstash.

## Available pipelines

|Name|Use Case|Description|
|----|--------|-----------|
|ms_to_ns| ms to ns conversion | Converts miliseconds to nanoseconds |
|x509_client_certificates| X.509 client certificate parsing | Parses a X.509 client distinguished name (DN) into ECS fields | 



## How to use a pipeline

1. **Find a pipeline**: Browse the `pipelines` directory to find a snippet that matches your needs.
2. **Create the pipeline in Elasticsearch**: Copy the contents of the `pipeline.json` file. Use the ingest API and optionally dev tools in Kibana to create the pipeline in you cluster.
Replace `<pipeline_name>` with a pipeline name.

    ```bash
    PUT _ingest/pipeline/<pipeline_name>
    # >> Paste the contents of pipeline.json here <<
    ```

3. **Test the pipeline (optional)**: Use the `_simulate` API with the example body from `simulate_example.json` to verify the pipeline works as expected.
    ```bash
    POST /_ingest/pipeline/<pipeline_name>/_simulate
    # >> Paste the contents of the simulate_example.json here <<
    ```
4. **Apply the pipeline**: Add the pipeline to the relevant index template or add the `pipeline` query parameter to the indexing requests.

## Contributing

Contributions are welcome! Please follow the existing directory structure when adding a new pipeline or pipeline snippet. Each new pipeline should include:

1. A descriptive directory name.
2. A `pipeline.json` file containing the pipeline.
3. A `simulate_example.json` file, containing the body for the `_ingest/pipeline/<pipeline_name>/_simulate` input with example logging.
4. A `simulate_results.json` file, containing the exact results after running the ingest pipeline simulation. This will be used as unit tests.
5. Last but not least, a `README.md` containing a description and an example of an input and output document only containing the fields needed/produced by the pipeline. Also contains all the custom fields (not present in ECS) used in that pipeline.

See existing pipelines and snippets for examples.
