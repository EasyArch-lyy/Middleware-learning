package com.example.elasticsearchtest.service;

import com.example.elasticsearchtest.bean.PoiBean;
import com.example.elasticsearchtest.bean.User;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class TestService {

    private final static Logger LOGGER = LoggerFactory.getLogger(TestService.class);
    private static RestHighLevelClient restClient = null;
    private String eshost = "127.0.0.1:9200";

    @PostConstruct
    public void init() {
        restClient = initRestHighLevelClient(eshost);
        LOGGER.info("初始化restESClient成功");
    }

    private RestHighLevelClient initRestHighLevelClient(String eshost) {
        List<HttpHost> httpHosts = new ArrayList<>();
        String[] address = eshost.split(":");
        httpHosts.add(new HttpHost(address[0], Integer.parseInt(address[1]), "http"));
        HttpHost[] httpHostsArr = new HttpHost[httpHosts.size()];
        return new RestHighLevelClient(RestClient.builder(httpHosts.toArray(httpHostsArr)));
    }

    /**
     * 销毁bean时执行
     */
    @PreDestroy
    public void close() {
        if (restClient != null) {
            try {
                restClient.close();
            } catch (IOException e) {
                LOGGER.info(e.getMessage());
            }
        }
    }

    /**
     * 向ES中创建索引
     *
     * @param client
     * @param indexName
     */
    public static void createIndex(RestHighLevelClient client, String indexName) {

        LOGGER.info("开始创建索引...");
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 3)        // 设置分片数
                .put("index.number_of_replicas", 2));    // 设置副本数

        createIndexRequest.mapping(
                "  {\n" +
//                            "    \""+typeName+"\": {\n" +   7版本es不需要
                        "      \"properties\": {\n" +
                        "        \"id\": {\n" +
                        "          \"type\": \"integer\"\n" +
                        "        },\n" +
                        "        \"name\": {\n" +
                        "          \"type\": \"text\"\n" +
                        "        },\n" +
                        "        \"location\": {\n" +
                        "          \"type\": \"geo_point\"\n" +
                        "        }\n" +
                        "    }\n" +
                        "  }",
                XContentType.JSON);
        // 为索引创建一个别名
        createIndexRequest.alias(new Alias(indexName + "_alias"));
        CreateIndexResponse createIndexResponse = null;
        try {
            createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            boolean acknowledged = createIndexResponse.isAcknowledged();
            boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
            LOGGER.info("acknowledged:{}", acknowledged);
            LOGGER.info("shardsAcknowledged:{}", shardsAcknowledged);
            LOGGER.info("创建索引成功！");
        } catch (IOException e) {
            LOGGER.warn("创建索引失败！");
        }
    }

    /**
     * 向索引添加数据
     *
     * @param client
     * @param indexName
     */
    public static Integer addIndexData(RestHighLevelClient client, String indexName) {

        Map<String, String> result = new HashMap<>();
        client = client == null ? restClient : client;
        indexName = StringUtils.isEmpty(indexName) ? "test" : indexName;
        LOGGER.info("开始导入数据, indexName：" + indexName + "; typeName：" + ";");
        int line = 0;
        BulkRequest request = null;
        try {
            request = new BulkRequest();
            List<User> cityList = new ArrayList<>();
            double lat = 39.929986;
            double lon = 116.395645;
            for (int i = 0; i < 100; i++) {
                double max = 0.00001;
                double min = 0.000001;
                Random random = new Random();
                double s = random.nextDouble() % (max - min + 1) + max;
                DecimalFormat df = new DecimalFormat("######0.000000");
                String lons = df.format(s + lon);
                String lats = df.format(s + lat);
                Double dlon = Double.valueOf(lons);
                Double dlat = Double.valueOf(lats);
                User user = new User(i, "马歇尔·D·蒂奇" + i, dlat, dlon);
                cityList.add(user);
            }
            for (int i = 0; i < cityList.size(); i++) {
                line++;
                // 纬度在前经度在后
                String location = cityList.get(i).getLocation()[0] + "," + cityList.get(i).getLocation()[1];
                request.add(new IndexRequest(indexName).source(XContentType.JSON, "id", cityList.get(i).getId(), "name", cityList.get(i).getName(), "location", location));
                if (line % 10 == 0) {
                    LOGGER.info("导入第{}条数据：{}", line, cityList.get(i));
                }
                if (line % 100 == 0) {
                    BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
                    request = new BulkRequest();
                    LOGGER.info("-------------成功导入前{}条停靠点数据----------------", line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return request.numberOfActions();
    }

    public static String toJSONData(User user) {
        String jsonData = null;
        try {
            XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
            jsonBuild.startObject()
                    .field("id", user.getId())
                    .field("name", user.getName())
                    .startArray("location")
                    .value(user.getLon())
                    .value(user.getLat())
                    .endArray()
                    .endObject();
            jsonData = jsonBuild.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jsonData;
    }

    /**
     * 创建mapping
     *
     * @param indexType
     */
    public static XContentBuilder createMapping(String indexType) {

        XContentBuilder mapping = null;
        try {
            mapping = jsonBuilder().startObject()
                    // 索引库名（类似数据库中的表）
                    .startObject("properties")
                    // ID
                    .startObject("id").field("type", "long").endObject()
                    // 姓名
                    .startObject("name").field("type", "string").endObject()
                    // 位置
                    .startObject("location").field("type", "geo_point").endObject()
                    .endObject().endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mapping;
    }

    /**
     * 索引搜索
     *
     * @param indexName
     * @param lat
     * @param lon
     */
    public static void getNearPeopleByDistance(String indexName, double lat, double lon) {

        String field = "location";
        List<PoiBean> result = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        GeoPoint point = new GeoPoint(lat, lon);
        GeoDistanceQueryBuilder geoDistanceQueryBuilder = QueryBuilders.geoDistanceQuery(field)
                .point(point)
                .distance(10, DistanceUnit.METERS)
                .geoDistance(GeoDistance.ARC);  // 选择按弧线ARC或水平线PLANE

        // 获取距离多少公里
        GeoDistanceSortBuilder sortBuilder = new GeoDistanceSortBuilder(field, point)
                .unit(DistanceUnit.METERS)
                .order(SortOrder.ASC)
                .geoDistance(GeoDistance.ARC);

        searchSourceBuilder.postFilter(geoDistanceQueryBuilder);
        searchSourceBuilder.sort(sortBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        // 将请求体加入到请求中
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = restClient.search(searchRequest, RequestOptions.DEFAULT);
            RestStatus status = searchResponse.status();
            // 处理搜索命中文档结果
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            Float usetime = searchResponse.getTook().getMillis() / 1000f;
            LOGGER.info("搜索附近的人(" + hits.getTotalHits() + "个)，耗时(" + usetime + "秒)：");
            for (SearchHit hit : searchHits) {
                String hitIndex = hit.getIndex();
                String hitType = hit.getType();
                String id = hit.getId();
                float score = hit.getScore();
                // 取用来排序的值
                Object[] sortRes = hit.getSortValues();
                Double hitDistance = Double.parseDouble(sortRes[0].toString());
                PoiBean hitPoi = new PoiBean();
                hitPoi.setId(id);
                hitPoi.setIndex(hitIndex);
                hitPoi.setType(hitType);
                hitPoi.setScore(score);
                hitPoi.setDistance(hitDistance);
                result.add(hitPoi);
                LOGGER.info("index:" + hitIndex + "  type:" + hitType + "  id:" + id + " distance:" + hitDistance);
                String sourceAsString = hit.getSourceAsString(); //取成json串
                LOGGER.info(sourceAsString);
            }
        } catch (IOException e) {
            LOGGER.error("es 查询错误：", e);
        }
    }

    public static void main(String[] args) throws IOException {
        TestService testService = new TestService();
        testService.init();
        String index = "search_distance_test_1";
        double lat = 40.200158;
        double lon = 116.665817;
        // 创建索引
//        createIndex(restClient, index);
        // 添加数据
//        addIndexData(restClient, index);
        long start = System.currentTimeMillis();
        getNearPeopleByDistance(index, lat, lon);
        long end = System.currentTimeMillis();
        LOGGER.info((end - start) + "毫秒");
        restClient.close();
    }
}

