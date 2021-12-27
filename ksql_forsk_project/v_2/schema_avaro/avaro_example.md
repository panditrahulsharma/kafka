## help links
    https://docs.confluent.io/platform/current/control-center/topics/schema.html
    
    https://docs.confluent.io/platform/current/schema-registry/schema_registry_tutorial.html?utm_source=github&utm_medium=demo&utm_campaign=ch.examples_type.community_content.clients-avro
    
    https://github.com/confluentinc/examples/blob/7.0.1-post/clients/avro/src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment.avsc


## avro format sample
    https://github.com/confluentinc/examples/blob/7.0.1-post/clients/avro/src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment.avsc


    {
    "namespace": "io.confluent.examples.clients.basicavro",
    "type": "record",
    "name": "Payment",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "double"}
    ]
    }