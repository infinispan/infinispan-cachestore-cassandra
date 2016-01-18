package org.infinispan.persistence.cassandra.configuration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser80;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.kohsuke.MetaInfServices;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

/**
 * Cassandra cache store configuration parser.
 *
 * @author Jakub Markos
 * @since 8.0
 */
@MetaInfServices
@Namespaces({
      @Namespace(uri = "urn:infinispan:config:store:cassandra:8.2", root = "cassandra-store"),
      @Namespace(root = "cassandra-store")
})
public class CassandraStoreConfigurationParser82 implements ConfigurationParser {

   public CassandraStoreConfigurationParser82() {
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
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

   private void parseCassandraStore(final XMLExtendedStreamReader reader,
                                    PersistenceConfigurationBuilder persistenceBuilder) throws XMLStreamException {
      CassandraStoreConfigurationBuilder builder = new CassandraStoreConfigurationBuilder(persistenceBuilder);
      parseCassAttributes(reader, builder);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
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
               Parser80.parseStoreElement(reader, builder);
               break;
            }
         }
      }
      persistenceBuilder.addStore(builder);
   }

   private void parseConnectionPool(XMLExtendedStreamReader reader, CassandraStoreConnectionPoolConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case POOL_TIMEOUT_MILLIS: {
               builder.poolTimeoutMillis(Integer.parseInt(value));
               break;
            }
            case HEARTBEAT_INTERVAL_SECONDS: {
               builder.heartbeatIntervalSeconds(Integer.parseInt(value));
               break;
            }
            case IDLE_TIMEOUT_SECONDS: {
               builder.idleTimeoutSeconds(Integer.parseInt(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseServer(XMLExtendedStreamReader reader, CassandraStoreServerConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
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

   private void parseCassAttributes(XMLExtendedStreamReader reader, CassandraStoreConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         String attrName = reader.getAttributeLocalName(i);
         Attribute attribute = Attribute.forName(attrName);
         switch (attribute) {
            case AUTO_CREATE_KEYSPACE:
               builder.autoCreateKeyspace(Boolean.parseBoolean(value));
               break;
            case KEYSPACE:
               builder.keyspace(value);
               break;
            case ENTRY_TABLE:
               builder.entryTable(value);
               break;
            default: {
               Parser80.parseStoreAttribute(reader, i, builder);
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