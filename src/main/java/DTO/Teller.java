package DTO;
import lombok.Data;
@Data
public class Teller {
    private int valueDate;
    private String currency1;
    private String transactionCode;
    private String id;
    private int amountFcy1;
    private int amountFcy2;
    private int rate2;
    private String customer2;
    private String authoriser;
    private String opType;
    private String account1;
    private String account2;

    public Teller(){};

    public Teller(int valueDate, String currency1, String transactionCode,
                  String id, int amountFcy1, int amountFcy2, int rate2,
                  String customer2, String authoriser, String opType,
                  String account1, String account2) {
        this.valueDate = valueDate;
        this.currency1 = currency1;
        this.transactionCode = transactionCode;
        this.id = id;
        this.amountFcy1 = amountFcy1;
        this.amountFcy2 = amountFcy2;
        this.rate2 = rate2;
        this.customer2 = customer2;
        this.authoriser = authoriser;
        this.opType = opType;
        this.account1 = account1;
        this.account2 = account2;
    }
}

