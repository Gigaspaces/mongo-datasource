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

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.client.spaceproxy.metadata.TypeDescFactory;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorVersionedSerializationUtils;
import com.gigaspaces.persistency.error.SpaceMongoDataSourceException;
import com.gigaspaces.persistency.error.SpaceMongoException;
import com.gigaspaces.persistency.metadata.*;
import com.gigaspaces.sync.AddIndexData;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.IntroduceTypeData;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.mongodb.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.persistency.support.SpaceTypeDescriptorContainer;
import org.openspaces.persistency.support.TypeDescriptorUtils;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * MongoDB driver client wrapper
 * 
 * @author Shadi Massalha
 */
public class MongoClientConnector {

    private static final String DOLLAR_SIGN = "__d_s__";
    private static final String TYPE_DESCRIPTOR_FIELD_NAME = "value";
    private static final String METADATA_COLLECTION_NAME = "metadata";

    private static final Log logger = LogFactory
            .getLog(MongoClientConnector.class);

    private final MongoClient client;
    private final String dbName;
    private final IndexBuilder indexBuilder;
    private final PojoRepository repository = new PojoRepository();

    // TODO: shadi must add documentation
    private static final Map<String, SpaceTypeDescriptorContainer> types = new ConcurrentHashMap<String, SpaceTypeDescriptorContainer>();
    private static final Map<String, SpaceDocumentMapper<DBObject>> mappingCache = new ConcurrentHashMap<String, SpaceDocumentMapper<DBObject>>();

    public MongoClientConnector(MongoClient client, String db) {

        this.client = client;
        this.dbName = db;
        this.indexBuilder = new IndexBuilder(this);
    }

    public void close() throws IOException {

        client.close();
    }

    public void introduceType(IntroduceTypeData introduceTypeData) {

        introduceType(introduceTypeData.getTypeDescriptor());
    }

    public void introduceType(SpaceTypeDescriptor typeDescriptor) {

        DBCollection m = getConnection()
                .getCollection(METADATA_COLLECTION_NAME);

        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start().add(
                Constants.ID_PROPERTY, typeDescriptor.getTypeName());
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            IOUtils.writeObject(out,
                    SpaceTypeDescriptorVersionedSerializationUtils
                            .toSerializableForm(typeDescriptor));

            builder.add(TYPE_DESCRIPTOR_FIELD_NAME, bos.toByteArray());

            WriteResult wr = m.save(builder.get());

            if (logger.isTraceEnabled())
                logger.trace(wr);

            indexBuilder.ensureIndexes(typeDescriptor);

        } catch (IOException e) {
            logger.error(e);

            throw new SpaceMongoException(
                    "error occurs while serialize and save type descriptor: "
                            + typeDescriptor, e);
        }
    }

    public DB getConnection() {
        return client.getDB(dbName);
    }

    public DBCollection getCollection(String collectionName) {

        return getConnection().getCollection(
                collectionName.replace("$", DOLLAR_SIGN));
    }

    public void performBatch(DataSyncOperation[] operations) {
        List<BatchUnit> rows = new LinkedList<BatchUnit>();

        for (DataSyncOperation operation : operations) {

            BatchUnit bu = new BatchUnit();
            cacheTypeDescriptor(operation.getTypeDescriptor());
            bu.setSpaceDocument(toSpaceDocument(operation));
            bu.setDataSyncOperationType(operation.getDataSyncOperationType());
            rows.add(bu);
        }

        performBatch(rows);
    }

    private SpaceDocument toSpaceDocument(DataSyncOperation operation) {
        SpaceDocument spaceDocument = operation.getDataAsDocument();
        if (operation.supportsDataAsObject()) {
            Object obj = operation.getDataAsObject();
            fillInJava8Dates(spaceDocument, obj);
        }
        return spaceDocument;
    }

    private void fillInJava8Dates(SpaceDocument spaceDocument, Object obj) {
        spaceDocument.getProperties().forEach((k, v) -> {
            if (v instanceof SpaceDocument) {
                if (isaJava8Date((SpaceDocument) v)) {
                    try {
                        Object newVal = repository.getGetter(obj.getClass(), k).get(obj);
                        spaceDocument.setProperty(k, newVal);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        logger.error(e);
                        throw new SpaceMongoException("error occurs while filling SpaceDocument with Java8 dates", e);
                    }
                } else {
                    try {
                        Object property = repository.getGetter(obj.getClass(), k).get(obj);
                        fillInJava8Dates((SpaceDocument) v, property);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        logger.error(e);
                        throw new SpaceMongoException("error occurs while filling SpaceDocument with Java8 dates", e);
                    }
                }
            }
        });
    }

    private boolean isaJava8Date(SpaceDocument v) {
        return v.getTypeName().equals(ZonedDateTime.class.getName()) ||
                v.getTypeName().equals(LocalDateTime.class.getName()) ||
                v.getTypeName().equals(LocalDate.class.getName()) ||
                v.getTypeName().equals(LocalTime.class.getName());
    }
    
    public void performBatch(List<BatchUnit> rows) {
        if (logger.isTraceEnabled()) {
            logger.trace("MongoClientWrapper.performBatch(" + rows + ")");
            logger.trace("Batch size to be performed is " + rows.size());
        }
        //List<Future<? extends Number>> pending = new ArrayList<Future<? extends Number>>();

        for (BatchUnit row : rows) {
            SpaceDocument spaceDoc = row.getSpaceDocument();
            SpaceTypeDescriptor typeDescriptor = types.get(row.getTypeName())
                    .getTypeDescriptor();
            SpaceDocumentMapper<DBObject> mapper = getMapper(typeDescriptor);

            DBObject obj = mapper.toDBObject(spaceDoc);

            DBCollection col = getCollection(row.getTypeName());
            switch (row.getDataSyncOperationType()) {

            case WRITE:
            case UPDATE:
                col.save(obj);
                break;
            case PARTIAL_UPDATE:
                DBObject query = BasicDBObjectBuilder
                        .start()
                        .add(Constants.ID_PROPERTY,
                                obj.get(Constants.ID_PROPERTY)).get();
                
                DBObject update = normalize(obj);
                col.update(query, update);
                break;
            // case REMOVE_BY_UID: // Not supported by this implementation
            case REMOVE:
                col.remove(obj);
                break;
            default:
                throw new IllegalStateException(
                        "Unsupported data sync operation type: "
                                + row.getDataSyncOperationType());
            }
        }

        /*long totalCount = waitFor(pending);

        if (logger.isTraceEnabled()) {
            logger.trace("total accepted replies is: " + totalCount);
        }*/
    }

    public Collection<SpaceTypeDescriptor> loadMetadata(boolean reloadTypeDescriptors) {

        DBCollection metadata = getCollection(METADATA_COLLECTION_NAME);

        DBCursor cursor = metadata.find(new BasicDBObject());

        TypeDescFactory typeDescFactory = new TypeDescFactory();
        
        while (cursor.hasNext()) {
            DBObject type = cursor.next();
            String typeName = (String) type.get(Constants.ID_PROPERTY);
            Object b = type.get(TYPE_DESCRIPTOR_FIELD_NAME);

            readMetadata(typeName, b, typeDescFactory, reloadTypeDescriptors);
        }

        return getSortedTypes();
    }

    public Collection<SpaceTypeDescriptor> getSortedTypes() {

        return TypeDescriptorUtils.sort(types);
    }

    private void cacheTypeDescriptor(SpaceTypeDescriptor typeDescriptor) {

        if (typeDescriptor == null)
            throw new IllegalArgumentException("typeDescriptor can not be null");

        if (!types.containsKey(typeDescriptor.getTypeName()))
            introduceType(typeDescriptor);

        types.put(typeDescriptor.getTypeName(),
                new SpaceTypeDescriptorContainer(typeDescriptor));
    }

    private void readMetadata(String typeName, Object b, TypeDescFactory typeDescFactory, boolean reloadTypeDescriptors) {
        try {
            SpaceTypeDescriptor typeDescriptor = reloadTypeDescriptors ? loadOrCreate(typeName, b, typeDescFactory) : load(b);
            
            indexBuilder.ensureIndexes(typeDescriptor);

            cacheTypeDescriptor(typeDescriptor);

        } catch (ClassNotFoundException e) {
            logger.error(e);
            throw new SpaceMongoDataSourceException("Failed to deserialize: " + b, e);
        } catch (IOException e) {
            logger.error(e);
            throw new SpaceMongoDataSourceException("Failed to deserialize: " + b, e);
        }
    }
    
    private SpaceTypeDescriptor load(Object b) throws IOException, ClassNotFoundException {
        ObjectInput in = null;
        try {
            in = new ClassLoaderAwareInputStream(new ByteArrayInputStream((byte[]) b));
            Serializable typeDescWrapper = IOUtils.readObject(in);
            return SpaceTypeDescriptorVersionedSerializationUtils.fromSerializableForm(typeDescWrapper);
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    private SpaceTypeDescriptor loadOrCreate(String typeName, Object b, TypeDescFactory typeDescFactory) throws IOException, ClassNotFoundException {
        try {
            SpaceTypeDescriptor persistedTypeDescriptor = load(b);
            ITypeDesc spaceTypeDescriptor = createSpaceTypeDescriptor(typeName, typeDescFactory);
            if (persistedTypeDescriptor instanceof ITypeDesc && ((ITypeDesc) persistedTypeDescriptor).getChecksum() != spaceTypeDescriptor.getChecksum()) {
                logTypeDescriptors(typeName, (ITypeDesc)persistedTypeDescriptor, spaceTypeDescriptor);
                return spaceTypeDescriptor;
            }
            return persistedTypeDescriptor;
        } catch (SpaceMetadataException e) {
            logger.trace("Failed to deserialize persisted space type descriptor. Using space type descriptor for class type: " + typeName, e);
            ITypeDesc spaceTypeDescriptor = createSpaceTypeDescriptor(typeName, typeDescFactory);
            final StringBuilder sb = new StringBuilder();
            printTypeDesc(sb, spaceTypeDescriptor, "Space type descriptor:\n");
            logger.trace(sb.toString());
            return spaceTypeDescriptor;
        }
    }
    
    private static void logTypeDescriptors(String typeName, ITypeDesc persistedTypeDescriptor, ITypeDesc spaceTypeDescriptor) {
        final StringBuilder sb = new StringBuilder();
        sb.append("The persisted type descriptor is incompatible with the type description stored in the space.\n");
        sb.append(String.format("Type=[%s], Persisted type descriptor checksum=[%d], Space type descriptor checksum=[%d].%n", typeName, persistedTypeDescriptor.getChecksum(), spaceTypeDescriptor.getChecksum()));
        printTypeDesc(sb, persistedTypeDescriptor, "Persisted type descriptor:\n");
        sb.append("\n");
        printTypeDesc(sb, spaceTypeDescriptor, "Space type descriptor:\n");
        logger.trace(sb.toString());
    }
    
    private static void printTypeDesc(StringBuilder sb, ITypeDesc typeDesc, String header) {
        final String[] superClasses = typeDesc.getSuperClassesNames();
        final PropertyInfo[] properties = typeDesc.getProperties();
    
        sb.append(header);
        sb.append(String.format("Super classes: %d%n", superClasses.length));
        for (int i = 0; i < superClasses.length; i++)
            sb.append(String.format("%4d: Type=[%s]%n", i + 1, superClasses[i]));
    
        sb.append(String.format("Properties: %d%n", properties.length));
        for (int i = 0; i < properties.length; i++)
            sb.append(String.format("%4d: Name=[%s], Type=[%s]%n", i + 1, properties[i].getName(), properties[i].getTypeName()));
    
        sb.append(String.format("Checksum: %d.%n", typeDesc.getChecksum()));
    }

    private ITypeDesc createSpaceTypeDescriptor(String typeName, TypeDescFactory typeDescFactory) throws ClassNotFoundException {
        Class<?> type = ClassLoaderHelper.loadClass(typeName);
        return typeDescFactory.createPojoTypeDesc(type, null, null);
    }

    public void ensureIndexes(AddIndexData addIndexData) {
        indexBuilder.ensureIndexes(addIndexData);
    }

    private static SpaceDocumentMapper<DBObject> getMapper(
            SpaceTypeDescriptor typeDescriptor) {

        SpaceDocumentMapper<DBObject> mapper = mappingCache.get(typeDescriptor
                .getTypeName());
        if (mapper == null) {
            mapper = new DefaultSpaceDocumentMapper(typeDescriptor);
            mappingCache.put(typeDescriptor.getTypeName(), mapper);
        }

        return mapper;
    }

    private static DBObject normalize(DBObject obj) {
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();

        Iterator<String> iterator = obj.keySet().iterator();
        builder.push("$set");
        while (iterator.hasNext()) {

            String key = iterator.next();

            if (Constants.ID_PROPERTY.equals(key))
                continue;

            Object value = obj.get(key);

            if (value == null)
                continue;
            
            builder.add(key, value);
        }

        return builder.get();
    }

    private static long waitFor(List<Future<? extends Number>> replies) {

        long total = 0;

        for (Future<? extends Number> future : replies) {
            try {
                total += future.get().longValue();
            } catch (InterruptedException e) {
                throw new SpaceMongoException("Number of async operations: "
                        + replies.size(), e);
            } catch (ExecutionException e) {
                throw new SpaceMongoException("Number of async operations: "
                        + replies.size(), e);
            }
        }

        return total;
    }

    private static class ClassLoaderAwareInputStream extends ObjectInputStream{

        private ClassLoaderAwareInputStream(InputStream in) throws IOException {
            super(in);
        }

        @Override
        public Class resolveClass(ObjectStreamClass desc) throws IOException,
                ClassNotFoundException {
            ClassLoader currentTccl = null;
            try {
                currentTccl = Thread.currentThread().getContextClassLoader();
                return currentTccl.loadClass(desc.getName());
            } catch (Exception e) {
            }
            return super.resolveClass(desc);
        }
    }


}
