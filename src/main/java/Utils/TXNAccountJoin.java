package Utils;

import DTO.*;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class TXNAccountJoin extends CoProcessFunction<TXN, Account, TXNAccount> {
    private MapState<String,TXN> txnMapState;
    private MapState<String,Account> accountMapState;
    @Override
    public void open(Configuration config){
        txnMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("txn", String.class, TXN.class));
        accountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("account", String.class, Account.class));

    }
    @Override
    public void processElement1(TXN txn, Context ctx, Collector<TXNAccount> out) throws Exception{
        if (accountMapState.contains(txn.getId())){
            Account account = accountMapState.get(txn.getId());
            String record_stat = getRecordStat(account.getOpType());
            out.collect(new TXNAccount(account.getOpenValDatedBal(),account.getCoCode(),account.getId(),
                    account.getCustomer(),record_stat,account.getWorkingBalance(),txn.getTxn_amount()));
        }
        else{
            txnMapState.put(txn.getId(),txn);
        }
    }
    public void processElement2(Account account, Context ctx, Collector<TXNAccount> out) throws Exception {
        accountMapState.put(account.getId(), account);
        // Kiểm tra xem có TXN nào đang chờ account này không
//        for (TXN txn : txnMapState.values()) {
//            if (txn.getId().equals(account.getId())) {
//                Account ac = accountMapState.get(txn.getId());
//
//                txnMapState.remove(txn.getId());
//
//            }
//        }
    }
    public String getRecordStat(String op_type) {
        if (op_type == null) {
            return null;
        }
        if (!op_type.equals("D")) {
            return "O";
        } else {
            return "C";
        }
    }

}
