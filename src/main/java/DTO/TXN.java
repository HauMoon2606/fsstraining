package DTO;
import lombok.Data;
@Data
public class TXN {
    private String id;
    private int txn_amount;

    public TXN(){};

    public TXN(String id, int txn_amount) {
        this.id = id;
        this.txn_amount = txn_amount;
    }
}
