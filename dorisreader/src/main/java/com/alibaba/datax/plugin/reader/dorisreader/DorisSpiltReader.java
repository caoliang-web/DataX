package com.alibaba.datax.plugin.reader.dorisreader;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class DorisSpiltReader {

    private static final Logger LOG = LoggerFactory.getLogger(DorisReader.Job.class);
    private static final String API_PREFIX = "/api";
    private static final String SCHEMA = "_schema";
    private static final String QUERY_PLAN = "_query_plan";




    private static String getLoadHost(Keys options) {
        List<String> hostList = options.getReadUrlList();
        Collections.shuffle(hostList);
        String host = new StringBuilder("http://").append(hostList.get((0))).toString();
        if (checkConnection(host)){
            return host;
        }
        return null;
    }


    private static boolean checkConnection(String host) {
        try {
            URL url = new URL(host);
            HttpURLConnection co =  (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(5000);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e1) {
            e1.printStackTrace();
            return false;
        }
    }


    public static List<PartitionDefinition> findPartitions(Keys options) throws DorisReadException {
        String host = getLoadHost(options);
        if(host == null){
            throw new DorisReadException ("load_url cannot be empty, or the host cannot connect.Please check your configuration.");
        }

        String readUrl = new StringBuilder(host)
                .append(API_PREFIX)
                .append("/")
                .append(options.getDatabase())
                .append("/")
                .append(options.getTable())
                .append("/")
                .append(QUERY_PLAN)
                .toString();

        HttpPost httpPost = new HttpPost(readUrl);
        String entity = "{\"sql\": \"" + options.getQuerySql() + "\"}";
        LOG.info("Query SQL Sending to Doris FE is: '{}'.", options.getQuerySql());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Post body Sending to Doris FE is: '{}'.", entity);
        }
        StringEntity stringEntity = new StringEntity(entity, StandardCharsets.UTF_8);
        stringEntity.setContentEncoding("UTF-8");
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);
        String resStr = send(httpPost, options);
        QueryPlan queryPlan = getQueryPlan(resStr);
        Map<String, List<Long>> be2Tablets = selectBeForTablet(queryPlan);
        return tabletsMapToPartition(be2Tablets, options, queryPlan.getOpaquedQueryPlan());
    }


    /**
     * translate BE tablets map to Doris RDD partition.
     *
     * @param be2Tablets
     * @param options
     * @param queryPlan
     * @return
     */
    private static List<PartitionDefinition> tabletsMapToPartition(Map<String, List<Long>> be2Tablets, Keys options, String queryPlan) {
        int tabletsSize = options.getRequestTabletSize();
        List<PartitionDefinition> partitionDefinitions = new ArrayList<>();
        for (Map.Entry<String, List<Long>> beInfo : be2Tablets.entrySet()) {
            HashSet<Long> tabletSet = new HashSet<>(beInfo.getValue());
            beInfo.getValue().clear();
            beInfo.getValue().addAll(tabletSet);

            int first = 0;
            while (first < beInfo.getValue().size()) {
                Set<Long> partitionsTablets =
                        new HashSet<>(beInfo.getValue().subList(
                                       first,
                                       Math.min(beInfo.getValue().size(), first + tabletsSize)));

                first = first + tabletsSize;
                PartitionDefinition partitionDefinition =
                        new PartitionDefinition(options.getDatabase(),
                                                options.getTable(),
                                                beInfo.getKey(),
                                                partitionsTablets,
                                                queryPlan);

                partitionDefinitions.add(partitionDefinition);

            }

        }

        return partitionDefinitions;
    }


    /**
     * select which Doris BE to get tablet data
     *
     * @param queryPlan
     * @return
     */
    private static Map<String, List<Long>> selectBeForTablet(QueryPlan queryPlan) {
        Map<String, List<Long>> be2Tablets = new HashMap<>();
        for (Map.Entry<String, Tablet> partition : queryPlan.getPartitions().entrySet()) {
            long tabletId;
            try {
                tabletId = Long.parseLong(partition.getKey());
            } catch (NumberFormatException e) {
                String errMsg = "Parse tablet id '" + partition.getKey() + "' to long failed.";
                throw new DorisReadException(errMsg, e);
            }

            String target = null;
            int tabletCount = Integer.MAX_VALUE;
            for (String candidate : partition.getValue().getRoutings()) {
                if (!be2Tablets.containsKey(candidate)) {
                    List<Long> tablets = new ArrayList<>();
                    be2Tablets.put(candidate, tablets);
                    target = candidate;
                    break;
                } else {
                    if (be2Tablets.get(candidate).size() < tabletCount) {
                        target = candidate;
                        tabletCount = be2Tablets.get(candidate).size();
                    }
                }
            }

            if (target == null) {
                String errMsg = "Cannot choice Doris BE for tablet " + tabletId;
                throw new DorisReadException(errMsg);
            }
            be2Tablets.get(target).add(tabletId);
        }
        return be2Tablets;
    }


    /**
     * translate Doris FE response string to inner {@link QueryPlan} struct.
     *
     * @param response
     * @return
     */

    private static QueryPlan getQueryPlan(String response) {
        ObjectMapper objectMapper = new ObjectMapper();
        QueryPlan queryPlan;

        try {
            queryPlan = objectMapper.readValue(response, QueryPlan.class);
        } catch (JsonParseException e) {
            String errMsg = "Doris FE's response is not a json. res: " + response;
            throw new DorisReadException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "Doris FE's response cannot map to schema. res: " + response;
            throw new DorisReadException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse Doris FE's response to json failed. res: " + response;
            throw new DorisReadException(errMsg, e);
        }

        if (queryPlan == null) {
            throw new DorisReadException("queryPlan is null.");
        }
        return queryPlan;

    }


    /**
     * send request to Doris FE and get response json string.
     *
     * @param request
     * @param options
     * @return
     * @throws DorisReadException
     */
    private static String send(HttpRequestBase request, Keys options) throws DorisReadException {
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(options.getDorisRequestConnectTimeout())
                        .setSocketTimeout(options.getDorisRequestReadTimeout())
                        .build();

        request.setConfig(requestConfig);
        IOException ex = null;
        int statusCode = -1;

        for (int attempt = 0; attempt < options.getRetries(); attempt++) {

            try {
                String response = getConnectionPost(request, options.getUsername(), options.getPassword());
                if (response == null) {
                    LOG.warn(
                            "Failed to get response from Doris FE {}, http code is {}",
                            request.getURI(),
                            statusCode);
                    continue;
                }
                LOG.info("Success get response from Doris FE: {}, response is: {}.",
                        request.getURI(),
                        response);

                ObjectMapper objectMapper = new ObjectMapper();
                Map map = objectMapper.readValue(response, Map.class);
                if (map.containsKey("code") && map.containsKey("msg")) {
                    Object data = map.get("data");
                    return objectMapper.writeValueAsString(data);
                } else {
                    return response;
                }
            } catch (IOException e) {
                ex = e;
                LOG.warn("Connect to doris {} failed.", request.getURI(), e);
            }
        }

        LOG.error("Connect to doris {} failed.", request.getURI(), ex);
        throw new DorisReadException(request.getURI().toString(), statusCode, ex);

    }


    private static String getConnectionPost(HttpRequestBase request, String username, String password) throws IOException {
        URL url = new URL(request.getURI().toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod(request.getMethod());
        String authEncoding =
                Base64.getEncoder()
                        .encodeToString(
                                String.format("%s:%s", username, password)
                                        .getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        InputStream content = ((HttpPost) request).getEntity().getContent();
        String res = IOUtils.toString(content);
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setConnectTimeout(request.getConfig().getConnectTimeout());
        conn.setReadTimeout(request.getConfig().getSocketTimeout());
        PrintWriter out = new PrintWriter(conn.getOutputStream());
        // send request params
        out.print(res);
        // flush
        out.flush();
        // read response
        return parseResponse(conn);
    }

    private static String parseResponse(HttpURLConnection connection)
            throws IOException {
        if (connection.getResponseCode() != HttpStatus.SC_OK) {
            LOG.warn(
                    "Failed to get response from Doris  {}, http code is {}",
                    connection.getURL(),
                    connection.getResponseCode());
            throw new IOException("Failed to get response from Doris");
        }
        StringBuffer result = new StringBuffer();
        try (Scanner scanner = new Scanner(connection.getInputStream(), "utf-8")) {
            while (scanner.hasNext()) {
                result.append(scanner.next());
            }
            return result.toString();
        }
    }

}
