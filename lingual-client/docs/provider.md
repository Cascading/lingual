
# Overview

The provider mechanism allows for new protocols and formats to be added dynamically to a Lingual query.

A protocol is a label specifying a Cascading Tap, and a format is a label for a Cascading Scheme.

By default Lingual supports multiple text formats like 'csv' and 'tsv' (comma separated values, and tab separated values,
respectively). Files ending with '.csv' or '.tsv' are automatically mapped to the proper format.

And Cascading local mode supports the 'file' protocol via the FileTap, and Hadoop mode supports the 'hdfs' protocol
via the Hfs Tap. URLs (identifiers) starting with 'file:' or 'hdfs:' are automatically madded to the proper protocol.

# Creating

To add new protocol or format providers, a new Jar library must be created with an optional 'factory' class and a
`provider.properties` file defining how to find and use the factory.

If a factory class is not provided, the `extends` property must be used to specify an existing provider that these
properties will ammend.

# Detail

    cascading.bind.provider.names=
    cascading.bind.provider.[provider_name].platforms=

    # one or the other
    cascading.bind.provider.[provider_name].factory.classname=[classname]
    cascading.bind.provider.[provider_name].extends=[provider_name]

    # define protocols differentiated by properties
    cascading.bind.provider.[provider_name].protocol.names=
    cascading.bind.provider.[provider_name].protocol.[protocol_name].uris=
    cascading.bind.provider.[provider_name].protocol.[protocol_name].[property]=[value]

    # define formats differentiated by properties
    cascading.bind.provider.[provider_name].format.names=
    cascading.bind.provider.[provider_name].format.[format_name].extensions=
    cascading.bind.provider.[provider_name].format.[format_name].[property]=[value]

    # optional format property specifying the protocols this format is compatible with
    # otherwise all defined protocols in this defintion will be used
    cascading.bind.provider.[provider_name].format.[format_name].protocols=

