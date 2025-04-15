package Utils;

import DTO.*;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class TXNAccountBalJoin extends CoProcessFunction<TXNAccount, ODS_CRB_BAL, TXNAccountBal> {

    private MapState<String, TXNAccount> txnAccountMapState;
    private MapState<String, ODS_CRB_BAL> balMapState;

    @Override
    public void open(Configuration config) {
        txnAccountMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("txnAccount", String.class, TXNAccount.class)
        );
        balMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("bal", String.class, ODS_CRB_BAL.class)
        );
    }

    @Override
    public void processElement1(TXNAccount txnAccount, Context ctx, Collector<TXNAccountBal> out) throws Exception {
        String key = txnAccount.getCustAcNo();
        if (balMapState.contains(key)) {
            ODS_CRB_BAL ods_crb_bal = balMapState.get(key);
            if (ods_crb_bal != null) {
                out.collect(new TXNAccountBal(
                        txnAccount.getAcyLcyBalance(),
                        txnAccount.getBranchCode(),
                        txnAccount.getCustAcNo(),
                        txnAccount.getCustNo(),
                        ods_crb_bal.getLcy_bal(),
                        txnAccount.getRecordStat(),
                        txnAccount.getLcyCurrBalance(),
                        txnAccount.getMovementAmountTdy()
                ));
            } else {
                System.out.println("ods_crb_bal is null for key: " + key);
            }
        } else {
            txnAccountMapState.put(key, txnAccount);
        }
    }

    @Override
    public void processElement2(ODS_CRB_BAL ods_crb_bal, Context ctx, Collector<TXNAccountBal> out) throws Exception {
        String key = ods_crb_bal.getArrangement_nbr();  // Đây là CustAcNo tương ứng
        balMapState.put(key, ods_crb_bal);
    }
}
