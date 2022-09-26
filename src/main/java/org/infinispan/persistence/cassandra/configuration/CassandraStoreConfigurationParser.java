package org.infinispan.persistence.cassandra.configuration;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.kohsuke.MetaInfServices;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;
import static org.infinispan.persistence.cassandra.configuration.CassandraStoreConfigurationParser.NAMESPACE;

/**
 * Cassandra cache store configuration parser.
 *
 * @author Jakub Markos
 * @since 8.0
 */
@MetaInfServices
@Namespace(root = "cassandra-store")
@Namespace(uri = NAMESPACE + "*", root = "cassandra-store")
public class CassandraStoreConfigurationParser implements ConfigurationParser {

    static final String NAMESPACE = Parser.NAMESPACE + "store:cassandra:";


    public CassandraStoreConfigurationParser() {
    }

    @Override
    public void readElement(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
        ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

        Element element = Element.forName(reader.getLocalName());
        switch (element) {
            case CASSANDRA_STORE: {
                parseCassandraStore(reader, builder.persistence());
                break;
            }
            default: {
                throw ParseUtils.unexpectedElement(reader);
            }
        }
    }

    private void parseCassandraStore(final ConfigurationReader reader,
                                     PersistenceConfigurationBuilder persistenceBuilder) {
        CassandraStoreConfigurationBuilder builder = new CassandraStoreConfigurationBuilder(persistenceBuilder);
        parseCassAttributes(reader, builder);

        while (reader.hasNext() && (reader.nextElement() != ConfigurationReader.ElementType.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case CONNECTION_POOL: {
                    parseConnectionPool(reader, builder.connectionPool());
                    break;
                }
                case CASSANDRA_SERVER: {
                    parseServer(reader, builder.addServer());
                    break;
                }
                default: {
                    Parser.parseStoreElement(reader, builder);
                    break;
                }
            }
        }
        persistenceBuilder.addStore(builder);
    }

    private void parseConnectionPool(ConfigurationReader reader, CassandraStoreConnectionPoolConfigurationBuilder builder) {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            String value = replaceProperties(reader.getAttributeValue(i));
            Attribute attribute = Attribute.forName(reader.getAttributeName(i));
            switch (attribute) {
                case HEARTBEAT_TIMEOUT_MS: {
                    builder.heartbeatTimeoutMs(Integer.parseInt(value));
                    break;
                }
                case HEARTBEAT_INTERVAL_SECONDS: {
                    builder.heartbeatIntervalSeconds(Integer.parseInt(value));
                    break;
                }
                case LOCAL_SIZE: {
                    builder.localSize(Integer.parseInt(value));
                    break;
                }
                case REMOTE_SIZE: {
                    builder.remoteSize(Integer.parseInt(value));
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
    }

    private void parseServer(ConfigurationReader reader, CassandraStoreServerConfigurationBuilder builder) {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            String value = replaceProperties(reader.getAttributeValue(i));
            Attribute attribute = Attribute.forName(reader.getAttributeName(i));
            switch (attribute) {
                case HOST:
                    builder.host(value);
                    break;
                case PORT:
                    builder.port(Integer.parseInt(value));
                    break;
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
        }
        ParseUtils.requireNoContent(reader);
    }

    private void parseCassAttributes(ConfigurationReader reader, CassandraStoreConfigurationBuilder builder) {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            String value = replaceProperties(reader.getAttributeValue(i));
            String attrName = reader.getAttributeName(i);
            Attribute attribute = Attribute.forName(attrName);
            switch (attribute) {
                case AUTO_CREATE_KEYSPACE:
                    builder.autoCreateKeyspace(Boolean.parseBoolean(value));
                    break;
                case LOCAL_DATACENTER:
                    builder.localDatacenter(value);
                    break;
                case KEYSPACE:
                    builder.keyspace(value);
                    break;
                case ENTRY_TABLE:
                    builder.entryTable(value);
                    break;
                case READ_CONSISTENCY_LEVEL:
                    builder.readConsistencyLevel(DefaultConsistencyLevel.valueOf(value));
                    break;
                case READ_SERIAL_CONSISTENCY_LEVEL:
                    builder.readSerialConsistencyLevel(DefaultConsistencyLevel.valueOf(value));
                    break;
                case WRITE_CONSISTENCY_LEVEL:
                    builder.writeConsistencyLevel(DefaultConsistencyLevel.valueOf(value));
                    break;
                case WRITE_SERIAL_CONSISTENCY_LEVEL:
                    builder.writeSerialConsistencyLevel(DefaultConsistencyLevel.valueOf(value));
                    break;
                case REPLICATION_STRATEGY:
                    builder.replicationStrategy(value);
                    break;
                case COMPRESSION:
                    builder.compression(value);
                    break;
                case USE_SSL:
                    builder.useSsl(Boolean.parseBoolean(value));
                    break;
                case USERNAME:
                    builder.username(value);
                    break;
                case PASSWORD:
                    builder.password(value);
                    break;
                default: {
                    Parser.parseStoreAttribute(reader, i, builder);
                    break;
                }
            }
        }
    }


    @Override
    public Namespace[] getNamespaces() {
        return ParseUtils.getNamespaceAnnotations(getClass());
    }

}
