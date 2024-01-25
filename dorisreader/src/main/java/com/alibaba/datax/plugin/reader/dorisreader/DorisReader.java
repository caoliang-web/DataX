package com.alibaba.datax.plugin.reader.dorisreader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.fastjson2.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DorisReader extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.Doris;


    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration configuration = null;
        private Keys options;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;
        private List<PartitionDefinition> partitions;


        @Override
        public void init() {
            configuration = super.getPluginJobConf();
            options = new Keys(super.getPluginJobConf());
            options.validate();
            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.configuration);

        }

        @Override
        public void destroy() {

        }

        @Override
        public void preCheck() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<>();
            int elementsPerThread = partitions.size() / adviceNumber;
            for (int i = 0; i < adviceNumber; i++) {
                int startIndex = i * elementsPerThread;
                int endIndex = (i == adviceNumber - 1) ? partitions.size() : (startIndex + elementsPerThread);
                List<PartitionDefinition> taskDorisPartitions = partitions.subList(startIndex, endIndex);
                Configuration splitedConfig = this.configuration.clone();
                splitedConfig.set(Keys.PARTITION, taskDorisPartitions);
                configurations.add(splitedConfig);
            }

            return configurations;
        }

        @Override
        public void prepare() {
            try {
                partitions = DorisSpiltReader.findPartitions(options);
            } catch (DorisReadException e) {
                throw new RuntimeException(e.getMessage());
            }
        }

        @Override
        public void post() {


        }


    }

    public static class Task extends Reader.Task {
        private static Logger LOG = LoggerFactory.getLogger(Reader.Task.class);
        private Keys options;
        private List<PartitionDefinition> taskDorisPartitions;
        private Configuration taskConfiguration;


        @Override
        public void init() {
            taskConfiguration = super.getPluginJobConf();
            options = new Keys(super.getPluginJobConf());
            taskDorisPartitions = fromJson(this.taskConfiguration.get(Keys.PARTITION).toString(),new TypeReference<List<PartitionDefinition>>() {} );
        }

        public static <T> T fromJson(String json, TypeReference<T> typeRef) {
            return JSON.parseObject(json, typeRef);
        }

        @Override
        public void destroy() {

        }

        @Override
        public void prepare() {

        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("read start");
            for (PartitionDefinition partitionDefinition : this.taskDorisPartitions) {
                try (DorisValueReader dorisValueReader = new DorisValueReader(options, partitionDefinition,recordSender)){
                    while (dorisValueReader.hasNext()){
                        dorisValueReader.next();
                    }
                } catch (Exception e) {
                    LOG.error("doris reader failed {}" ,e);
                    throw new DorisReadException(e);
                }

            }

        }
    }

}
