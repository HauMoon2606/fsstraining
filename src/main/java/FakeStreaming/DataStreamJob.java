package FakeStreaming;
import DTO.*;
import Deserializer.JSONValueDeserializationDbzCompany;
import Deserializer.JSONValueDeserializationDbzCompanyBranchMap;
import Utils.DBUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.Data;
import java.sql.Connection;
import java.sql.DriverManager;


public class DataStreamJob {
	private static Connection conn;
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// topic debezium kafka
		String topicCompany  = "postgres-server.public.stgt24_company";
		String topicCompanyBranchMap = "postgres-server.public.cic_company_branch_map";
		// Config DB
		String postgresUrl = "jdbc:postgresql://localhost:5432/k6";
		String postgresUser = "k6";
		String postgresPass = "k6";
		//connect tới db
		conn = DriverManager.getConnection(postgresUrl, postgresUser, postgresPass);
		// topic company_branch_map từ kafka
		KafkaSource<DbzCompanyBranchMap> sourceCompanyBranchMap = KafkaSource.<DbzCompanyBranchMap>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics(topicCompanyBranchMap)
				.setGroupId("flink-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONValueDeserializationDbzCompanyBranchMap())
				.build();
		DataStream<DbzCompanyBranchMap> dbzCompanyBranchMapStream = env.fromSource(sourceCompanyBranchMap, WatermarkStrategy.noWatermarks(),"kafka-source-branchmap");
		DataStream<String> resultStream2 = dbzCompanyBranchMapStream.map(new MapFunction<DbzCompanyBranchMap, String>() {
			@Override
			public String map(DbzCompanyBranchMap dbzCompanyBranchMap) throws Exception {
				CompanyBranchMap companyBranchMap = dbzCompanyBranchMap.getAfterData();
				if(dbzCompanyBranchMap.getOp().equals("u")){
					DBUtils.modifyOldRecord(conn, companyBranchMap);
					DBUtils.addRecord(conn, companyBranchMap);
					return "success update company branch map";
				}
				else if(dbzCompanyBranchMap.getOp().equals("c")){
					DBUtils.modifyOldRecord(conn, companyBranchMap);
					DBUtils.addRecord(conn, companyBranchMap);
					return "success insert company branch map";
				}
				else {
					CompanyBranchMap oldCompanyBranchMap = dbzCompanyBranchMap.getBeforeData();
					DBUtils.deleteRecord(conn, oldCompanyBranchMap);
					return "success delete company branch map";
				}
			}
		});
		resultStream2.print();

		// topic company từ kafka
		KafkaSource<DbzCompany> source_company = KafkaSource.<DbzCompany>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics(topicCompany)
				.setGroupId("flink-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONValueDeserializationDbzCompany())
				.build();
		DataStream<DbzCompany> dbzStream = env.fromSource(source_company, WatermarkStrategy.noWatermarks(),"kafka-source-company");

		// join luồng company với db
		DataStream<BranchDim> branchDimDataStream = dbzStream.map(new MapFunction<DbzCompany, BranchDim>() {
			@Override
			public BranchDim map(DbzCompany dbzCompany) throws Exception {
				Company company = dbzCompany.getAfterData();
				return company.joinBranchMap();
			}
		});
		DataStream<String> resultDataStream = branchDimDataStream.map(new MapFunction<BranchDim, String>() {
			@Override
			public String map(BranchDim branchDim) throws Exception {
				branchDim.addOrUpdate(conn, branchDim);
				return "success";
			}
		});
		env.execute("test");
	}
}
