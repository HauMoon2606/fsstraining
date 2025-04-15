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
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;


//public class DataStreamJob {
//	private static Connection conn;
//	public static void main(String[] args) throws Exception {
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		// topic debezium kafka
//		String topicCompany  = "postgres-server.public.stgt24_company";
//		String topicCompanyBranchMap = "postgres-server.public.cic_company_branch_map";
//		// Config DB
//		String postgresUrl = "jdbc:postgresql://localhost:5432/k6";
//		String postgresUser = "k6";
//		String postgresPass = "k6";
//		//connect tới db
//		conn = DriverManager.getConnection(postgresUrl, postgresUser, postgresPass);
//		// topic company_branch_map từ kafka
//		KafkaSource<DbzCompanyBranchMap> sourceCompanyBranchMap = KafkaSource.<DbzCompanyBranchMap>builder()
//				.setBootstrapServers("localhost:9092")
//				.setTopics(topicCompanyBranchMap)
//				.setGroupId("flink-group")
//				.setStartingOffsets(OffsetsInitializer.earliest())
//				.setValueOnlyDeserializer(new JSONValueDeserializationDbzCompanyBranchMap())
//				.build();
//		DataStream<DbzCompanyBranchMap> dbzCompanyBranchMapStream = env.fromSource(sourceCompanyBranchMap, WatermarkStrategy.noWatermarks(),"kafka-source-branchmap");
//		DataStream<String> resultStream2 = dbzCompanyBranchMapStream.map(new MapFunction<DbzCompanyBranchMap, String>() {
//			@Override
//			public String map(DbzCompanyBranchMap dbzCompanyBranchMap) throws Exception {
//				CompanyBranchMap companyBranchMap = dbzCompanyBranchMap.getAfterData();
//				if(dbzCompanyBranchMap.getOp().equals("u")){
//					DBUtils.modifyOldRecord(conn, companyBranchMap);
//					DBUtils.addRecord(conn, companyBranchMap);
//					return "success update company branch map";
//				}
//				else if(dbzCompanyBranchMap.getOp().equals("c")){
//					DBUtils.modifyOldRecord(conn, companyBranchMap);
//					DBUtils.addRecord(conn, companyBranchMap);
//					return "success insert company branch map";
//				}
//				else {
//					CompanyBranchMap oldCompanyBranchMap = dbzCompanyBranchMap.getBeforeData();
//					DBUtils.deleteRecord(conn, oldCompanyBranchMap);
//					return "success delete company branch map";
//				}
//			}
//		});
//		resultStream2.print();
//
//		// topic company từ kafka
//		KafkaSource<DbzCompany> source_company = KafkaSource.<DbzCompany>builder()
//				.setBootstrapServers("localhost:9092")
//				.setTopics(topicCompany)
//				.setGroupId("flink-group")
//				.setStartingOffsets(OffsetsInitializer.earliest())
//				.setValueOnlyDeserializer(new JSONValueDeserializationDbzCompany())
//				.build();
//		DataStream<DbzCompany> dbzStream = env.fromSource(source_company, WatermarkStrategy.noWatermarks(),"kafka-source-company");
//
//		// join luồng company với db
//		DataStream<BranchDim> branchDimDataStream = dbzStream.map(new MapFunction<DbzCompany, BranchDim>() {
//			@Override
//			public BranchDim map(DbzCompany dbzCompany) throws Exception {
//				Company company = dbzCompany.getAfterData();
//				return company.joinBranchMap();
//			}
//		});
//		DataStream<String> resultDataStream = branchDimDataStream.map(new MapFunction<BranchDim, String>() {
//			@Override
//			public String map(BranchDim branchDim) throws Exception {
//				branchDim.addOrUpdate(conn, branchDim);
//				return "success";
//			}
//		});
//		env.execute("test");
//	}
//}


// Ví dụ join 2 datastream dùng CoProcessFuncion, có mapstate lưu trữ tạm thời

//import org.apache.flink.api.common.state.MapState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
//import org.apache.flink.util.Collector;
//
//public class DataStreamJob {
//
//	public static void main(String[] args) throws Exception {
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		// Tạo 2 luồng dữ liệu mẫu
//		DataStream<Order> ordersStream = env.fromElements(
//				new Order("order1", "cust1"),
//				new Order("order2", "cust2")
//		);
//
//		DataStream<Customer> customersStream = env.fromElements(
//				new Customer("cust1", "Alice"),
//				new Customer("cust2", "Bob")
//		);
//
//		// Join 2 stream bằng CoProcessFunction
//		ordersStream
//				.keyBy(order -> order.customerId)
//				.connect(customersStream.keyBy(customer -> customer.customerId))
//				.process(new OrderCustomerJoin())
//				.print();
//
//		env.execute("Join Orders and Customers Streams");
//	}
//
//	// ----------- Class đơn hàng -----------
//	public static class Order {
//		public String orderId;
//		public String customerId;
//
//		// Constructor mặc định (bắt buộc cho Flink)
//		public Order() {}
//
//		public Order(String orderId, String customerId) {
//			this.orderId = orderId;
//			this.customerId = customerId;
//		}
//	}
//
//	// ----------- Class khách hàng -----------
//	public static class Customer {
//		public String customerId;
//		public String customerName;
//
//		public Customer() {}
//
//		public Customer(String customerId, String customerName) {
//			this.customerId = customerId;
//			this.customerName = customerName;
//		}
//	}
//
//	// ----------- CoProcessFunction xử lý JOIN -----------
//	public static class OrderCustomerJoin extends CoProcessFunction<Order, Customer, String> {
//
//		private MapState<String, Order> orderState;
//		private MapState<String, Customer> customerState;
//
//		@Override
//		public void open(Configuration parameters) {
//			orderState = getRuntimeContext().getMapState(
//					new MapStateDescriptor<>("orders", String.class, Order.class)
//			);
//			customerState = getRuntimeContext().getMapState(
//					new MapStateDescriptor<>("customers", String.class, Customer.class)
//			);
//		}
//
//		@Override
//		public void processElement1(Order order, Context ctx, Collector<String> out) throws Exception {
//			if (customerState.contains(order.customerId)) {
//				Customer customer = customerState.get(order.customerId);
//				out.collect("Order " + order.orderId + " from " + customer.customerName);
//			} else {
//				orderState.put(order.customerId, order);
//			}
//		}
//
//		@Override
//		public void processElement2(Customer customer, Context ctx, Collector<String> out) throws Exception {
//			if (orderState.contains(customer.customerId)) {
//				Order order = orderState.get(customer.customerId);
//				out.collect("Order " + order.orderId + " from " + customer.customerName);
//			} else {
//				customerState.put(customer.customerId, customer);
//			}
//		}
//	}
//}

import DTO.ODS_CRB_BAL;
import Utils.Utils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import Utils.TellerAccountJoin;
import Utils.TXNAccountJoin;
import Utils.TXNAccountBalJoin;

public class DataStreamJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<ODS_CRB_BAL> odsCrbBalStream = env.fromElements(
				new ODS_CRB_BAL("1", 5000, "111", 4223),
				new ODS_CRB_BAL("2", 3200, "222", 4223),
				new ODS_CRB_BAL("3", 15000, "333", 4233),
				new ODS_CRB_BAL("4", 0, "12344", 4391)
		);
		DataStream<ODS_CRB_BAL> odsCrbBalFillerStream = odsCrbBalStream.filter(new FilterFunction<ODS_CRB_BAL>() {
			@Override
			public boolean filter(ODS_CRB_BAL odsCrbBal) throws Exception {
				int tmp = Integer.parseInt(Utils.NVL(String.valueOf(odsCrbBal.getCoa_code()),"0").substring(0,3));
				return tmp >420 && tmp <430;
			}
		});
		KeyedStream<ODS_CRB_BAL, String> odsCrbBalKeyedStream =
				odsCrbBalFillerStream.keyBy(new KeySelector<ODS_CRB_BAL, String>() {
					@Override
					public String getKey(ODS_CRB_BAL odsCrbBal){
						return odsCrbBal.getArrangement_nbr();
					}
				});
		DataStream<ODS_CRB_BAL> odsCrbBalReduceStream =
				odsCrbBalKeyedStream.reduce(new ReduceFunction<ODS_CRB_BAL>() {
					@Override
					public ODS_CRB_BAL reduce(ODS_CRB_BAL o1, ODS_CRB_BAL o2) throws Exception {
						return new ODS_CRB_BAL(o1.getLcy_bal()+o2.getLcy_bal(),o1.getArrangement_nbr(),o1.getCoa_code());
					}
				});
//		odsCrbBalReduceStream.print();

		DataStream<Account> accountStream = env.fromElements(
				new Account("111", "1", "21", "VND", 10000, Date.valueOf("2024-01-01"), "A", 2342348, "1008", "1008", "001"),
				new Account("222", "2", "35", "USD", 80000, Date.valueOf("2025-01-01"), "B", 12345678, "1005", "1005", "002"),
				new Account("333", "3", "42", "VND", 50000, Date.valueOf("2026-06-15"), "C", 9999999, "1008", "1005", "001"),
				new Account("444", "4", "56", "USD", 90000, Date.valueOf("2024-12-31"), "D", 7777777, "1005", "1008", "002"),
				new Account("5", "5", "30", "VND", 15000, Date.valueOf("2023-08-10"), "A", 888888888, "1005", "1005", "001")
		);
		DataStream<Teller> tellerStream = env.fromElements(
				new Teller(20240415, "VND", "T1234", "1", 1000, 300000, 1, "1", "auth01", "A", "111", "222"),
				new Teller(20240415,  "VND", "T1234", "2", 1500, 200000, 1, "2", "auth02", "B", "333", "444"),
				new Teller(20240415,  "USD", "T5678", "3", 500, 100000, 1, "3", "auth03", "C", "555", "666"),
				new Teller(20240415,  "VND", "T1111", "4", 700, 80000, 1, "4", "auth04", "D", "777", "888"),
				new Teller(20240415,  "USD", "T2222", "5", 2000, 900000, 1, "5", null,"A", "999", "000")
		);

		DataStream<TXN> tellerAccount1 = tellerStream
				.keyBy(Teller::getAccount1)
				.connect(accountStream.keyBy(Account::getId))
				.process(new TellerAccountJoin());

		DataStream<TXN> tellerAccount2 = tellerStream
				.keyBy(Teller::getAccount2)
				.connect(accountStream.keyBy(Account::getId))
				.process(new TellerAccountJoin());

		DataStream<TXN> txnStream = tellerAccount1.union(tellerAccount2);

		DataStream<TXN> txnResult = txnStream.keyBy(TXN::getId).reduce(new ReduceFunction<TXN>() {
			@Override
			public TXN reduce(TXN o1, TXN o2) throws Exception {
				return new TXN(o1.getId(), o1.getTxn_amount()+ o2.getTxn_amount());
			}
		});

		DataStream<TXNAccount> txnAccountStream = txnResult.keyBy(TXN::getId)
				.connect(accountStream.keyBy(Account::getId))
						.process(new TXNAccountJoin());

		DataStream<TXNAccountBal> resultStream = txnAccountStream.keyBy(TXNAccount::getCustAcNo)
						.connect(odsCrbBalReduceStream.keyBy(ODS_CRB_BAL::getArrangement_nbr))
								.process(new TXNAccountBalJoin());
		resultStream.print();

		env.execute("ODS_CRB_BAL Demo");
	}
}






