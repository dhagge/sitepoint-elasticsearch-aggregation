import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class IndexEs {

    static final List<String> STATES =
            Arrays.asList("AZ", "CA", "CO", "NM", "NV", "TX", "UT");
    static final List<String> CATEGORIES = Arrays.asList(
            "Cholla", "Barrel", "Hedgehog", "Prickly Pear", "Saguaro");

    static final DecimalFormat df2 = new DecimalFormat(".##");
    static final Random random = new Random();
    /**
     * Create a random dataset to index into elasticsearch.
     * @param bulkRequest The bulk request builder to add the dataset to
     * @return The document to index
     * @throws IOException
     */
    public static void createDataset(Client client,
                                     BulkRequestBuilder bulkRequest)
            throws IOException {
        // put 1000 random records into the dataset
        //  - a random price between 1 and 10,000
        //  - a random date within the past year
        //  - a random state in the southwest
        //  - a random category of cactus
        for (int i=0; i<1000; i++) {
            XContentBuilder data = jsonBuilder()
                .startObject()
                .field("price", Double.valueOf(df2.format(random.nextDouble() * (10000.0 - 1.0) + 1.0)))
                .field("date", LocalDate.now().minusDays(random.nextInt(365)))
                .field("state", STATES.get(random.nextInt(STATES.size())))
                .field("category", CATEGORIES.get(random.nextInt(CATEGORIES.size())))
                .endObject();
            IndexRequestBuilder builder =
                    client.prepareIndex("cactus", "sales").setSource(data);
            bulkRequest.add(builder);
        }
    }

    public static XContentBuilder createMapping() throws IOException {
        return jsonBuilder()
            .startObject()
                .startObject("sales")
                    .startObject("properties")
                        .startObject("price")
                            .field("type", "double")
                        .endObject()
                        .startObject("date")
                            .field("type", "date")
                        .endObject()
                        .startObject("state")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("category")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
    }

    /**
     * Create an elasticsearch client that connects to our instance
     * @return The client
     */
    public static TransportClient createClient() throws UnknownHostException {
        return new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(
                        InetAddress.getByName("localhost"), 9300));
    }

    public static void main(String[] args) throws IOException {
        TransportClient client = createClient();

        try {

            // create the mapping for our index
            client.admin().indices().prepareCreate("cactus")
                    .addMapping("sales", createMapping())
                    .execute().actionGet();

            // create a bulk request (which batches multiple requests into a single call)
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            createDataset(client, bulkRequest);

            // now call elasticsearch to index the documents
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                System.out.println("Oops, apparently cactus datasets are hard to create...");
                System.out.println(bulkResponse.buildFailureMessage());
            } else {
                System.out.println("Yay, we have lots of cactus sales!");
            }
        } finally {
            client.close();
        }
    }
}