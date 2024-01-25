package com.alibaba.datax.plugin.reader.dorisreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class Keys {

    public static final String PARTITION = "partition";
    private static final int DEFAULT_BATCH_SIZE = 4096;
    private static final int DEFAULT_QUERY_TIMEOUT = 3600;
    private static final long DEFAULT_MEM_LIMIT = 2147483648L;
    private static final int DEFAULT_REQUEST_TABLET_SIZE = Integer.MAX_VALUE;
    private static final int DEFAULT_TABLET_SIZE_MIN = 1;
    private static final int DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;
    private static final int DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;
    private static final int DEFAULT_RETRIES = 3;


    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "connection[0].selectedDatabase";
    private static final String TABLE = "connection[0].table[0]";
    private static final String COLUMN = "column";
    private static final String QUERY_SQL = "querySql";
    private static final String JDBC_URL = "connection[0].jdbcUrl";
    private static final String BATCH_SIZE = "batchSize";
    private static final String QUERY_TIMEOUT = "queryTimeOut";
    private static final String MEM_LIMIT = "memLimit";
    private static final String READ_URL = "readUrl";
    private static final String REQUEST_TABLET_SIZE = "requestTabletSize";
    private static final String WHERE = "where";
    private static final String DORIS_REQUEST_CONNECT_TIMEOUT_MS = "dorisRequestConnectTimeout";
    private static final String DORIS_REQUEST_READ_TIMEOUT_MS = "dorisRequestReadTimeout";


    private final Configuration options;


    public Keys(Configuration options) {
        this.options = options;

    }

    public String getJdbcUrl() {
        return options.getString(JDBC_URL);
    }

    public String getDatabase() {
        return options.getString(DATABASE);
    }

    public String getTable() {
        return options.getString(TABLE);
    }

    public String getUsername() {
        return options.getString(USERNAME);
    }

    public String getPassword() {
        return options.getString(PASSWORD);
    }

    public String getColumns() {
        return options.getString(COLUMN);
    }

    public int getRetries() {
        return DEFAULT_RETRIES;
    }

    public String getQuerySql() {

        if (StringUtils.isNotBlank(options.getString(QUERY_SQL))) {
            return options.getString(QUERY_SQL);
        } else {
            String readFiled = getColumns();
            String sql = "select " + readFiled + " from " + options.getString(DATABASE) + "." + options.getString(TABLE);
            if (!StringUtils.isEmpty(options.getString(WHERE))) {
                sql += " where " + options.getString(WHERE);
            }
            return sql;
        }
    }


    public void validate() {
        validateRequired();
        validateReadUrl();
    }

    public int getBatchRows() {
        Integer rows = options.getInt(BATCH_SIZE);
        return null == rows ? DEFAULT_BATCH_SIZE : rows;
    }

    public long getMemLimit() {
        Long size = options.getLong(MEM_LIMIT);
        return null == size ? DEFAULT_MEM_LIMIT : size;
    }

    public int getQueryTimeout() {
        Integer rows = options.getInt(QUERY_TIMEOUT);
        return null == rows ? DEFAULT_QUERY_TIMEOUT : rows;
    }

    public int getDorisRequestConnectTimeout() {
        Integer connectTimeout = options.getInt(DORIS_REQUEST_CONNECT_TIMEOUT_MS);
        return connectTimeout == null ? DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT : connectTimeout;
    }

    public int getDorisRequestReadTimeout() {
        Integer socketTimeout = options.getInt(DORIS_REQUEST_READ_TIMEOUT_MS);
        return socketTimeout == null ? DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT : socketTimeout;
    }

    public List<String> getReadUrlList() {
        return options.getList(READ_URL, String.class);
    }


    private void validateReadUrl() {
        List<String> urlList = getReadUrlList();
        for (String host : urlList) {
            if (host.split(":").length < 2) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        "The format of loadUrl is not correct, please enter:[`fe_ip:fe_http_ip;fe_ip:fe_http_ip`].");
            }
        }
    }


    private void validateRequired() {
        options.getNecessaryValue(READ_URL, DBUtilErrorCode.REQUIRED_VALUE);
    }

    public int getRequestTabletSize() {
        Integer rows = options.getInt(REQUEST_TABLET_SIZE) == null ? DEFAULT_REQUEST_TABLET_SIZE : options.getInt(REQUEST_TABLET_SIZE);
        if (rows < DEFAULT_TABLET_SIZE_MIN || rows > DEFAULT_REQUEST_TABLET_SIZE) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                    "requestTabletSize value is between 1 and int maximum value");
        }
        return rows;
    }

}
