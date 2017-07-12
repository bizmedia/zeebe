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
package io.zeebe.broker.test;

import static io.zeebe.broker.task.TaskQueueServiceNames.taskQueueInstanceStreamProcessorServiceName;
import static io.zeebe.logstreams.log.LogStream.DEFAULT_LOG_NAME;

import java.io.InputStream;
import java.util.concurrent.*;
import java.util.function.Supplier;

import io.zeebe.broker.Broker;
import io.zeebe.broker.event.TopicSubscriptionServiceNames;
import io.zeebe.broker.transport.TransportServiceNames;
import io.zeebe.servicecontainer.*;
import org.junit.rules.ExternalResource;

public class EmbeddedBrokerRule extends ExternalResource
{
    protected Broker broker;

    protected Supplier<InputStream> configSupplier;

    public EmbeddedBrokerRule()
    {
        this("zeebe.unit-test.cfg.toml");
    }

    public EmbeddedBrokerRule(Supplier<InputStream> configSupplier)
    {
        this.configSupplier = configSupplier;
    }

    public EmbeddedBrokerRule(String configFileClasspathLocation)
    {
        this(() -> EmbeddedBrokerRule.class.getClassLoader().getResourceAsStream(configFileClasspathLocation));
    }

    @Override
    protected void before() throws Throwable
    {
        startBroker();
    }

    @Override
    protected void after()
    {
        stopBroker();
    }

    public Broker getBroker()
    {
        return this.broker;
    }

    public void restartBroker()
    {
        stopBroker();
        startBroker();
    }

    public void stopBroker()
    {
        broker.close();
        broker = null;
        System.gc();
    }

    public void startBroker()
    {
        broker = new Broker(configSupplier.get());

        final ServiceContainer serviceContainer = broker.getBrokerContext().getServiceContainer();

        try
        {
            // Hack: block until default task queue log has been installed
            // How to make it better: https://github.com/camunda-tngp/zeebe/issues/196
            serviceContainer.createService(TestService.NAME, new TestService())
                .dependency(TopicSubscriptionServiceNames.subscriptionManagementServiceName(DEFAULT_LOG_NAME))
                .dependency(TransportServiceNames.serverTransport(TransportServiceNames.CLIENT_API_SERVER_NAME))
                .dependency(taskQueueInstanceStreamProcessorServiceName(DEFAULT_LOG_NAME))
                .install()
                .get(25, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            stopBroker();
            throw new RuntimeException("Default task queue log not installed into the container withing 5 seconds.");
        }
    }

    public <T> void removeService(ServiceName<T> name)
    {
        try
        {
            broker.getBrokerContext().getServiceContainer()
                .removeService(name)
                .get(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            throw new RuntimeException("Could not remove service " + name.getName() + " in 10 seconds.");
        }
    }

    static class TestService implements Service<TestService>
    {

        static final ServiceName<TestService> NAME = ServiceName.newServiceName("testService", TestService.class);

        @Override
        public void start(ServiceStartContext startContext)
        {

        }

        @Override
        public void stop(ServiceStopContext stopContext)
        {

        }

        @Override
        public TestService get()
        {
            return this;
        }

    }

}