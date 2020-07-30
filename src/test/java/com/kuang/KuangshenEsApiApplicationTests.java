package com.kuang;

import com.alibaba.fastjson.JSON;
import com.kuang.pojo.User;
import com.kuang.utils.ESconst;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class KuangshenEsApiApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    //测试创建索引
    @Test
    void testCreateIndex() throws IOException {
        //创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("kuang_index");
        //客户端执行请求IndicesClient，请求后获得响应
        CreateIndexResponse createIndexResponse =
                restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    //测试获取索引
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("kuang_index");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //测试删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("kuang_index");
        //删除
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    //测试添加文档
    @Test
    void testAddDocument() throws IOException{
        //创建对象
        User user = new User("狂神说",3);
        //创建请求
        IndexRequest request = new IndexRequest("kuang_index");

        //规则 put/kuang_index/_doc/1
        request.id("1");
        //request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        //将我们的数据放入请求 json
        request.source(JSON.toJSONString(user),XContentType.JSON);

        //客户端发送请求，获取响应的结果
        IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);

        System.out.println(response.toString());
        System.out.println(response.status());
    }

    //获取文档，判断是否存在 get/index/_doc/1
    @Test
    void testIsExists() throws IOException{
        GetRequest getRequest = new GetRequest("kuang_index","1");

        //不获取返回的_source 的上下文了,可以不写
        //getRequest.fetchSourceContext(new FetchSourceContext(false));
        //getRequest.storedFields("_none_");//获取规则

        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //获取文档信息
    @Test
    void testGetDocument() throws IOException{
        GetRequest getRequest = new GetRequest("kuang_index", "1");
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);

        //打印文档的内容
        System.out.println(getResponse.getSourceAsString());
        //返回全部
        System.out.println(getResponse);
    }

    //更新文档的信息
    @Test
    void testUpdateRequest() throws IOException{
        UpdateRequest updateRequest = new UpdateRequest("kuang_index", "1");
        updateRequest.timeout("1s");

        User user = new User("狂神说java",18);
        updateRequest.doc(JSON.toJSONString(user),XContentType.JSON);

        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.status());

    }

    //删除文档记录
    @Test
    void testDeleteRequest() throws IOException{
        DeleteRequest deleteRequest = new DeleteRequest("kuang_index", "1");
        deleteRequest.timeout("1s");

        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }

    //特殊的，真的项目一般都会批量插入数据
    @Test
    void testBulkRequest() throws IOException{
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("kuangshen1",3));
        userList.add(new User("kuangshen2",3));
        userList.add(new User("kuangshen3",3));
        userList.add(new User("Young1",5));
        userList.add(new User("Young2",5));
        userList.add(new User("Young3",5));

        //批量处理请求
        for (int i = 0; i < userList.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("kuang_index")
                    //id不写，会自动生成随机id
                    //.id("" + (i+1))
                    .source(JSON.toJSONString(userList.get(i)),XContentType.JSON)
            );
        }

        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        //是否失败，返回false代表成功
        System.out.println(bulkResponse.hasFailures());
    }

    //查询
    @Test
    void testSearch() throws IOException{
        SearchRequest searchRequest = new SearchRequest(ESconst.ES_INDEX);
        //构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //查询条件，我们可以使用QueryBuilders 工具实现
        //QueryBuilders.termQuery 精确查询
        //QueryBuilders.matchAllQuery() 匹配所有

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "kuangshen1");
        //MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        //还可以构建各种分页
        //sourceBuilder.from(0);
        //sourceBuilder.size(1);
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));


        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println("=============================");
        for(SearchHit documentFields : searchResponse.getHits().getHits()){
            System.out.println(documentFields.getSourceAsMap());
        }
    }

}
