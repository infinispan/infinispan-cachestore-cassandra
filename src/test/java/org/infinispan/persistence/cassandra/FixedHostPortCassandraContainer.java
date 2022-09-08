package org.infinispan.persistence.cassandra;

import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Cassandra Container with the ability to add fixed port-mappings
 * Should be removed as soon as a feature to dynamically configure ports is implemented in CassandraStore-Config
 */
public class FixedHostPortCassandraContainer<SELF extends CassandraContainer<SELF>> extends CassandraContainer<SELF> {
    public FixedHostPortCassandraContainer() {
        super();
    }

    public FixedHostPortCassandraContainer(String dockerImageName) {
        super(dockerImageName);
    }

    public FixedHostPortCassandraContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    public SELF withFixedExposedPort(int hostPort, int containerPort) {
        super.addFixedExposedPort(hostPort, containerPort);

        return self();
    }
}
