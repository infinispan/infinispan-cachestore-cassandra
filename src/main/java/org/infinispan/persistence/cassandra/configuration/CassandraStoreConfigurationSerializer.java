package org.infinispan.persistence.cassandra.configuration;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.AbstractStoreSerializer;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

public class CassandraStoreConfigurationSerializer extends AbstractStoreSerializer implements ConfigurationSerializer<CassandraStoreConfiguration> {
    @Override
    public void serialize(ConfigurationWriter writer, CassandraStoreConfiguration configuration) {
        writer.writeStartElement(Element.CASSANDRA_STORE);
        writer.writeDefaultNamespace(CassandraStoreConfigurationParser.NAMESPACE + Version.getMajorMinor());
        configuration.attributes().write(writer);
        writeCommonStoreSubAttributes(writer, configuration);
        writeCommonStoreElements(writer, configuration);
        writer.writeEndElement();
    }
}
