package Utils;

import DTO.Account;
import DTO.TXN;
import DTO.Teller;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class TellerAccountJoin extends CoProcessFunction<Teller, Account, TXN> {
    private MapState<String, Account> accountState;
    private MapState<String, Teller> tellerState;
    @Override
    public void open(Configuration config){
        accountState = getRuntimeContext().getMapState(new MapStateDescriptor<>("account", String.class, Account.class));
        tellerState = getRuntimeContext().getMapState(new MapStateDescriptor<>("teller", String.class, Teller.class));

    }
    @Override
    public void processElement1(Teller teller, Context ctx, Collector<TXN> out) throws Exception{
        boolean matched = false;
        if (accountState.contains(teller.getAccount1())) {
            out.collect(new TXN(teller.getAccount1(), -1 * teller.getAmountFcy1()));
            matched = true;
        }

        if (accountState.contains(teller.getAccount2())) {
            out.collect(new TXN(teller.getAccount2(), teller.getAmountFcy1()));
            matched = true;
        }
        if (!matched) {
            tellerState.put(teller.getId(), teller);
        }
    }
    public void processElement2(Account account, Context ctx, Collector<TXN> out) throws Exception {
        accountState.put(account.getId(), account);
        // Kiểm tra xem có Teller nào đang chờ account này không
        for (Teller teller : tellerState.values()) {
            if (teller.getAccount1().equals(account.getId())) {
                out.collect(new TXN(teller.getAccount1(), -1 * teller.getAmountFcy1()));
                tellerState.remove(teller.getId());
            } else if (teller.getAccount2().equals(account.getId())) {
                out.collect(new TXN(teller.getAccount2(), teller.getAmountFcy1()));
                tellerState.remove(teller.getId());
            }
        }
    }

}
