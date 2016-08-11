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

public class MongoSpaceDataSourceConfigurer {

	private MongoClientConnector mongoClientConnector;
    private ClusterInfo clusterInfo;
    private boolean reloadPojoSchema;
    
	public MongoSpaceDataSourceConfigurer mongoClientConnector(MongoClientConnector mongoClientConnector) {
		this.mongoClientConnector = mongoClientConnector;
		return this;
	}

    public MongoSpaceDataSourceConfigurer clusterInfo(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
        return this;
    }
    
    public MongoSpaceDataSourceConfigurer reloadPojoSchema(boolean reloadPojoSchema) {
        this.reloadPojoSchema = reloadPojoSchema;
        return this;
    }

    public MongoSpaceDataSource create() {
        MongoSpaceDataSource datasource = new MongoSpaceDataSource(mongoClientConnector, clusterInfo);
        datasource.setReloadPojoSchema(reloadPojoSchema);
        return datasource;
	}
}
