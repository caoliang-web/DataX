package com.alibaba.datax.plugin.reader.dorisreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.fastjson2.JSON;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.doris.sdk.thrift.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DorisValueReader implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisValueReader.class);
    private static final String DORIS_DEFAULT_CLUSTER = "default_cluster";


    protected AtomicBoolean eos = new AtomicBoolean(false);
    private static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String DATETIMEV2_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private final DateTimeFormatter dateTimeFormatter =
            DateTimeFormatter.ofPattern(DATETIME_PATTERN);
    private final DateTimeFormatter dateTimeV2Formatter =
            DateTimeFormatter.ofPattern(DATETIMEV2_PATTERN);
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private ArrowStreamReader arrowStreamReader;
    private RootAllocator rootAllocator;
    protected int offset = 0;

    protected Lock clientLock = new ReentrantLock();
    protected String contextId;
    protected Schema schema;

    private int readRowCount = 0;
    private int rowCountInOneBatch;
    private int offsetInRowBatch = 0;
    private TScanOpenParams openParams;
    private BackendClient backendClient;
    private PartitionDefinition partition;
    private Keys options;
    private RecordSender recordSender;
    private List<Record> records = new ArrayList<>();

    public DorisValueReader(Keys options, PartitionDefinition partitionDefinition, RecordSender recordSender) {
        this.partition = partitionDefinition;
        this.options = options;
        this.backendClient = backendClient();
        this.recordSender = recordSender;
        init();
    }


    private void init() {
        clientLock.lock();
        try {
            this.openParams = openParams();
            TScanOpenResult openResult = this.backendClient.openScanner(this.openParams);
            this.contextId = openResult.getContextId();
            this.schema = convertToSchema(openResult.getSelectedColumns());
        } finally {
            clientLock.unlock();
        }
    }


    /**
     * read data.
     *
     * @return true if hax next value
     */
    public boolean hasNext() throws Exception {
        boolean hasNext = false;
        clientLock.lock();
        try {
            if (!eos.get() && (records.size() == 0 || !checkRowBatch())) {
                if (records.size() > 0) {
                    offset += readRowCount;
                    closeStream();
                }
                TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
                nextBatchParams.setContextId(contextId);
                nextBatchParams.setOffset(offset);
                TScanBatchResult nextResult = backendClient.getNext(nextBatchParams);
                eos.set(nextResult.isEos());
                if (!eos.get()) {
                    offsetInRowBatch = 0;
                    readRowCount = 0;
                    records.clear();
                    records = convertArrow(nextResult, schema);
                }
            }
            hasNext = !eos.get();
        } finally {
            clientLock.unlock();
        }
        return hasNext;
    }


    public void next() throws Exception {
        if (!hasNext()) {
            LOG.error("Should not come here.");
            throw new DorisReadException();
        }
        if (checkRowBatch()) {
            recordSender.sendToWriter(records.get(offsetInRowBatch++));
        }
    }

    public boolean checkRowBatch() {
        if (offsetInRowBatch < readRowCount) {
            return true;
        }
        return false;
    }


    private List<Record> convertArrow(TScanBatchResult nextResult, Schema schema) throws Exception {
        rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        arrowStreamReader = new ArrowStreamReader(new ByteArrayInputStream(nextResult.getRows()), rootAllocator);
        try {
            VectorSchemaRoot root = arrowStreamReader.getVectorSchemaRoot();
            while (arrowStreamReader.loadNextBatch()) {
                List<FieldVector> fieldVectors = root.getFieldVectors();
                //total data rows
                rowCountInOneBatch = root.getRowCount();
                //Arrow returns in column format and needs to be converted to row format
                for (int row = 0; row < rowCountInOneBatch; row++) {
                    Record record = recordSender.createRecord();
                    for (int col = 0; col < fieldVectors.size(); col++) {
                        FieldVector fieldVector = fieldVectors.get(col);
                        Types.MinorType minorType = fieldVector.getMinorType();
                        final String currentType = schema.get(col).getType();
                        Column column = convertValue(col, row, minorType, currentType, fieldVector);
                        record.addColumn(column);
                    }
                    records.add(record);
                }
                readRowCount += root.getRowCount();
            }
            return records;
        } catch (Exception e) {
            LOG.error("Read Doris Data failed because: ", e);
            throw new DorisReadException(e.getMessage());
        } finally {
           closeStream();
        }
    }

    private Column convertValue(int col, int rowIndex, Types.MinorType minorType, String currentType, FieldVector fieldVector) {
        Column column = null;
        switch (currentType) {
            case "NULL_TYPE":
                break;
            case "BOOLEAN":
                BitVector bitVector = (BitVector) fieldVector;
                Object fieldValue =
                        bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
                column = new BoolColumn((Boolean) fieldValue);
                break;
            case "TINYINT":
                TinyIntVector tinyIntVector = (TinyIntVector) fieldVector;
                fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
                column = new LongColumn((Long) fieldValue);
                break;
            case "SMALLINT":
                SmallIntVector smallIntVector = (SmallIntVector) fieldVector;
                fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
                column = new LongColumn((Long) fieldValue);
                break;
            case "INT":
                IntVector intVector = (IntVector) fieldVector;
                fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
                column = new LongColumn((Integer) fieldValue);
                break;
            case "BIGINT":
                BigIntVector bigIntVector = (BigIntVector) fieldVector;
                fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
                column = new LongColumn((Long) fieldValue);
                break;
            case "FLOAT":
                Float4Vector float4Vector = (Float4Vector) fieldVector;
                fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
                column = new DoubleColumn(BigDecimal.valueOf((Double) fieldValue));
                break;
            case "TIME":
            case "DOUBLE":
                Float8Vector float8Vector = (Float8Vector) fieldVector;
                fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
                column = new DoubleColumn(BigDecimal.valueOf((Double) fieldValue));
                break;
            case "BINARY":
                VarBinaryVector varBinaryVector = (VarBinaryVector) fieldVector;
                fieldValue =
                        varBinaryVector.isNull(rowIndex) ? null : varBinaryVector.get(rowIndex);
                column = new BytesColumn((byte[]) fieldValue);
                break;
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128I":
                DecimalVector decimalVector = (DecimalVector) fieldVector;
                if (decimalVector.isNull(rowIndex)) {
                    break;
                }
                BigDecimal value = decimalVector.getObject(rowIndex).stripTrailingZeros();
                column = new DoubleColumn(value);
                break;
            case "DATE":
            case "DATEV2":
                VarCharVector date = (VarCharVector) fieldVector;
                if (date.isNull(rowIndex)) {
                    break;
                }
                String stringValue = new String(date.get(rowIndex));
                LocalDate localDate = LocalDate.parse(stringValue, dateFormatter);
                column = new DateColumn(Date.valueOf(localDate));
                break;
            case "DATETIME":
                VarCharVector timeStampSecVector = (VarCharVector) fieldVector;
                if (timeStampSecVector.isNull(rowIndex)) {
                    break;
                }
                stringValue = new String(timeStampSecVector.get(rowIndex));
                LocalDateTime parse = LocalDateTime.parse(stringValue, dateTimeFormatter);
                column = new DateColumn(Date.valueOf(String.valueOf(parse)));
                break;
            case "DATETIMEV2":
                VarCharVector timeStampV2SecVector = (VarCharVector) fieldVector;
                if (timeStampV2SecVector.isNull(rowIndex)) {
                    break;
                }
                stringValue = new String(timeStampV2SecVector.get(rowIndex));
                stringValue = completeMilliseconds(stringValue);
                parse = LocalDateTime.parse(stringValue, dateTimeV2Formatter);
                column = new DateColumn(Date.valueOf(String.valueOf(parse)));
                break;
            case "LARGEINT":
                if (minorType.equals(Types.MinorType.FIXEDSIZEBINARY)) {
                    FixedSizeBinaryVector largeIntVector = (FixedSizeBinaryVector) fieldVector;
                    if (largeIntVector.isNull(rowIndex)) {
                        break;
                    }
                    byte[] bytes = largeIntVector.get(rowIndex);
                    int left = 0, right = bytes.length - 1;
                    while (left < right) {
                        byte temp = bytes[left];
                        bytes[left] = bytes[right];
                        bytes[right] = temp;
                        left++;
                        right--;
                    }
                    BigInteger largeInt = new BigInteger(bytes);
                    column = new LongColumn(largeInt);
                    break;
                } else {
                    VarCharVector largeIntVector = (VarCharVector) fieldVector;
                    if (largeIntVector.isNull(rowIndex)) {
                        column = null;
                        break;
                    }
                    stringValue = new String(largeIntVector.get(rowIndex));
                    BigInteger largeInt = new BigInteger(stringValue);
                    column = new LongColumn(largeInt);
                    break;
                }
            case "CHAR":
            case "VARCHAR":
            case "STRING":
            case "JSONB":
                VarCharVector varCharVector = (VarCharVector) fieldVector;
                if (varCharVector.isNull(rowIndex)) {
                    break;
                }
                stringValue = new String(varCharVector.get(rowIndex));
                column = new StringColumn(stringValue);
                break;
            case "ARRAY":
                ListVector listVector = (ListVector) fieldVector;
                Object listValue =
                        listVector.isNull(rowIndex) ? null : listVector.getObject(rowIndex);
                // todo: when the subtype of array is date, conversion is required
                column = new StringColumn(JSON.toJSONString(listValue));
                break;
            case "MAP":
                MapVector mapVector = (MapVector) fieldVector;
                UnionMapReader reader = mapVector.getReader();
                if (mapVector.isNull(rowIndex)) {
                    break;
                }
                reader.setPosition(rowIndex);
                Map<String, Object> mapValue = new HashMap<>();
                while (reader.next()) {
                    mapValue.put(reader.key().readObject().toString(), reader.value().readObject());
                }
                column = new StringColumn(JSON.toJSONString(mapValue));
                break;
            case "STRUCT":

                StructVector structVector = (StructVector) fieldVector;
                if (structVector.isNull(rowIndex)) {
                    break;
                }
                Map<String, ?> structValue = structVector.getObject(rowIndex);
                column = new StringColumn(JSON.toJSONString(structValue));
                break;
            default:
                String errMsg = "Unsupported type " + schema.get(col).getType();
                LOG.error(errMsg);
                throw new DorisReadException(errMsg);
        }
        return column;
    }

    private String completeMilliseconds(String stringValue) {
        if (stringValue.length() == DATETIMEV2_PATTERN.length()) {
            return stringValue;
        }
        StringBuilder sb = new StringBuilder(stringValue);
        if (stringValue.length() == DATETIME_PATTERN.length()) {
            sb.append(".");
        }
        while (sb.toString().length() < DATETIMEV2_PATTERN.length()) {
            sb.append(0);
        }
        return sb.toString();
    }

    /**
     * Client to request Doris BE
     *
     * @return
     */
    private BackendClient backendClient() {
        try {
            return new BackendClient(new Routing(partition.getBeAddress()), options);
        } catch (IllegalArgumentException e) {
            LOG.error("init backend:{} client failed,", partition.getBeAddress(), e);
            throw new DorisReadException(e);
        }
    }


    private TScanOpenParams openParams() {
        TScanOpenParams params = new TScanOpenParams();
        params.cluster = DORIS_DEFAULT_CLUSTER;
        params.database = partition.getDatabase();
        params.table = partition.getTable();
        params.tablet_ids = Arrays.asList(partition.getTabletIds().toArray(new Long[]{}));
        params.opaqued_query_plan = partition.getQueryPlan();
        params.setBatchSize(options.getBatchRows());
        params.setLimit(options.getMemLimit());
        params.setQueryTimeout(options.getQueryTimeout());
        params.setUser(options.getUsername());
        params.setPasswd(options.getPassword());
        return params;
    }


    /**
     * convert Doris return schema to inner schema struct.
     *
     * @param tscanColumnDescs Doris BE return schema
     * @return inner schema struct
     */
    public static Schema convertToSchema(List<TScanColumnDesc> tscanColumnDescs) {
        Schema schema = new Schema(tscanColumnDescs.size());
        tscanColumnDescs.stream()
                .forEach(
                        desc ->
                                schema.put(
                                        new Field(
                                                desc.getName(),
                                                desc.getType().name(),
                                                "",
                                                0,
                                                0,
                                                "")));
        return schema;
    }


    @Override
    public void close() throws Exception {
        clientLock.lock();
        try {
            TScanCloseParams closeParams = new TScanCloseParams();
            closeParams.setContextId(contextId);
            backendClient.closeScanner(closeParams);
        } finally {
            clientLock.unlock();
        }
    }

    public void closeStream() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException ioe) {
            // do nothing
        }
    }
}
