import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.net.UnknownHostException;
import java.text.DecimalFormat;

public class SalesByState {

    static final DecimalFormat PRICE_FORMATTER = new DecimalFormat(".##");
    static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy/MM/dd");

    public static void main(String[] args) throws UnknownHostException {
        TransportClient client = IndexEs.createClient();

        AggregationBuilder agg =
                AggregationBuilders.dateHistogram("salesByDate").field("date")
                .dateHistogramInterval(DateHistogramInterval.MONTH)
                .subAggregation(AggregationBuilders.sum("totalSales").field("price"));

        SearchResponse sr = client.prepareSearch("cactus")
                .addAggregation(agg)
                .execute().actionGet();

        MultiBucketsAggregation aggResp = sr.getAggregations().get("salesByDate");

        for (MultiBucketsAggregation.Bucket bucket : aggResp.getBuckets()) {
            DateTime key = (DateTime)bucket.getKey();
            InternalSum price = bucket.getAggregations().get("totalSales");
            System.out.println("Date: " + DATE_FORMATTER.print(key) +
                    ", Total Sales: " + PRICE_FORMATTER.format(price.getValue()));
        }
    }
}
