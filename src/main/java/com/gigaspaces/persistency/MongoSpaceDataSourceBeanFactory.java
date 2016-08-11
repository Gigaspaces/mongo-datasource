/*******************************************************************************
 * Copyright (c) 2012 GigaSpaces Technologies Ltd. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.gigaspaces.persistency;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

public class MongoSpaceDataSourceBeanFactory implements
		FactoryBean<MongoSpaceDataSource>, InitializingBean, DisposableBean ,ClusterInfoAware{

	private final MongoSpaceDataSourceConfigurer configurer = getConfigurer();

	private MongoSpaceDataSource mongoSpaceDataSource;

	public void setMongoClientConnector(MongoClientConnector mongoClientConnector) {
		configurer.mongoClientConnector(mongoClientConnector);
	}
	
	public void setReloadPojoSchema(boolean reloadPojoSchema) {
        configurer.reloadPojoSchema(reloadPojoSchema);
    }

	@Override
    public void destroy() throws Exception {
		mongoSpaceDataSource.close();
	}

	private MongoSpaceDataSourceConfigurer getConfigurer() {
		return new MongoSpaceDataSourceConfigurer();
	}

	@Override
    public void afterPropertiesSet() throws Exception {
		this.mongoSpaceDataSource = configurer.create();
	}

	@Override
    public MongoSpaceDataSource getObject() throws Exception {
		return mongoSpaceDataSource;
	}

	@Override
    public Class<?> getObjectType() {
		return MongoSpaceDataSource.class;
	}

	@Override
    public boolean isSingleton() {
		return true;
	}

    @Override
    public void setClusterInfo(ClusterInfo clusterInfo) {
        configurer.clusterInfo(clusterInfo);
    }
}
