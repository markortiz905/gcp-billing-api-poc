package com.mark.poc.GcpCostApi;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * this poc queries billing info from bq gcp once.
 * Note that bq billing reports are updated on daily basis.
 *
 * the service account must have access to bq with the following roles
 * - roles/bigquery.jobUser
 *
 * references:
 * - about bigquery iams - https://cloud.google.com/bigquery/docs/access-control
 *
 * @author Mark Ortiz
 */
@SpringBootApplication
public class GcpCostApiApplication {

	private Logger logger = LoggerFactory.getLogger(GcpCostApiApplication.class);

	@Autowired
	private BigQuery bigquery;

	public static void main(String[] args) {
		SpringApplication.run(GcpCostApiApplication.class, args);
	}

	/**
	 * <b>Total cost per invoice query<b/>
	 *
	 * This query shows the invoice total for each month, as a sum of regular costs, taxes, adjustments, and rounding errors.
	 *
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@EventListener(ApplicationReadyEvent.class)
	public void doSomethingAfterStartup() throws InterruptedException, IOException {
		String query = "SELECT\n" +
				" invoice.month,\n" +
				" (SUM(CAST(cost * 1000000 AS int64))\n" +
				"   + SUM(IFNULL((SELECT SUM(CAST(c.amount * 1000000 as int64))\n" +
				"                 FROM UNNEST(credits) c), 0))) / 1000000\n" +
				"   AS total_exact\n" +
				"FROM `project_costing.gcp_billing_export_resource_v1_01BFDE_7D4554_88DBC4`\n" +
				"GROUP BY 1\n" +
				"ORDER BY 1 ASC";
		QueryJobConfiguration queryConfig =
				QueryJobConfiguration.newBuilder(query).build();

		TableResult result = bigquery.query(queryConfig);

		List<Map<String, Object>> rows = new ArrayList<>();
		if(result.getTotalRows() == 1) {
			result.getValues().forEach(fieldValues -> {
				Map<String, Object> row = new HashMap<>();
				logger.info(fieldValues.get(0).getValue() + ", " + fieldValues.get(1).getValue());
				row.put("month", fieldValues.get(0).getValue());
				row.put("total_exact", fieldValues.get(1).getValue());
				rows.add(row);
			});
		} else {
			logger.info("No Billing Records at the moment.");
		}

		//ObjectMapper mapper = new ObjectMapper();
		//String rowsJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rows);

		//System.out.println(rowsJson);

	}

}
