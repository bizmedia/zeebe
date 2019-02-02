/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.clustering.base.gossip;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryBuilder;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.cluster.protocol.SwimMembershipProtocol;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.net.Address;
import io.zeebe.broker.Loggers;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.broker.system.configuration.NetworkCfg;
import io.zeebe.broker.system.configuration.SocketBindingCfg;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;

public class AtomixService implements Service<Atomix> {

  private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

  private final BrokerCfg configuration;
  private Atomix atomix;

  public AtomixService(final BrokerCfg configuration) {
    this.configuration = configuration;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    final ClusterCfg clusterCfg = configuration.getCluster();

    final int nodeId = clusterCfg.getNodeId();
    final String localMemberId = Integer.toString(nodeId);

    final NetworkCfg networkCfg = configuration.getNetwork();
    final String host = networkCfg.getAtomix().getHost();
    final int port = networkCfg.getAtomix().getPort();

    final NodeDiscoveryProvider discoveryProvider =
        createDiscoveryProvider(clusterCfg, localMemberId);
    final Properties properties = createNodeProperties(networkCfg);

    LOG.error("Setup atomix node in cluster {}", clusterCfg.getClusterName());

    final AtomixBuilder atomixBuilder =
        Atomix.builder()
            .withClusterId(clusterCfg.getClusterName())
            .withMembershipProtocol(
                SwimMembershipProtocol.builder()
                    .withFailureTimeout(Duration.ofMillis(2_000))
                    .build())
            .withMemberId(localMemberId)
            .withProperties(properties)
            .withAddress(Address.from(host, port))
            .withMembershipProvider(discoveryProvider);

    final RaftPartitionGroup partitionGroup =
        RaftPartitionGroup.builder("raft")
            .withMembers(
                Integer.toString(nodeId),
                Integer.toString((nodeId + 1) % 3),
                Integer.toString((nodeId + 2) % 3))
            .withDataDirectory(Files.createTempDir())
            .withNumPartitions(1)
            .withPartitionSize(3)
            .build();

    final RaftPartitionGroup systemGroup =
        RaftPartitionGroup.builder("system")
            .withMembers(
                Integer.toString(nodeId),
                Integer.toString((nodeId + 1) % 3),
                Integer.toString((nodeId + 2) % 3))
            .withDataDirectory(Files.createTempDir())
            .withNumPartitions(1)
            .withPartitionSize(3)
            .build();

    atomixBuilder.withManagementGroup(systemGroup).withPartitionGroups(partitionGroup);

    atomix = atomixBuilder.build();

    // only logging purpose for now
    atomix
        .getMembershipService()
        .addListener(mEvent -> LOG.info("Member {} receives {}", localMemberId, mEvent.toString()));
  }

  @Override
  public Atomix get() {
    return atomix;
  }

  private NodeDiscoveryProvider createDiscoveryProvider(
      ClusterCfg clusterCfg, String localMemberId) {
    final BootstrapDiscoveryBuilder builder = BootstrapDiscoveryProvider.builder();
    final List<String> initialContactPoints = clusterCfg.getInitialContactPoints();

    final List<Node> nodes = new ArrayList<>();
    initialContactPoints.forEach(
        contactAddress -> {
          final String[] address = contactAddress.split(":");
          final int memberPort = Integer.parseInt(address[1]);

          final Node node =
              Node.builder().withAddress(Address.from(address[0], memberPort)).build();
          LOG.info("Member {} will contact node: {}", localMemberId, node.address());
          nodes.add(node);
        });
    return builder.withNodes(nodes).build();
  }

  private Properties createNodeProperties(NetworkCfg networkCfg) {
    final Properties properties = new Properties();

    final ObjectMapper objectMapper = new ObjectMapper();
    try {
      addAddressToProperties(
          "replicationAddress", networkCfg.getReplication(), properties, objectMapper);
      addAddressToProperties(
          "subscriptionAddress", networkCfg.getSubscription(), properties, objectMapper);
      addAddressToProperties(
          "managementAddress", networkCfg.getManagement(), properties, objectMapper);
      addAddressToProperties("clientAddress", networkCfg.getClient(), properties, objectMapper);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return properties;
  }

  private void addAddressToProperties(
      String addressName,
      SocketBindingCfg socketBindingCfg,
      Properties properties,
      ObjectMapper objectMapper)
      throws JsonProcessingException {
    final InetSocketAddress inetSocketAddress =
        socketBindingCfg.toSocketAddress().toInetSocketAddress();
    final String value = objectMapper.writeValueAsString(inetSocketAddress);
    properties.setProperty(addressName, value);
  }
}
