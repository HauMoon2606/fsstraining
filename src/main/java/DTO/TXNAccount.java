package DTO;
import lombok.Data;
@Data
public class TXNAccount {
    private int acyLcyBalance;
    private String branchCode;
    private String custAcNo;
    private String custNo;
    private String recordStat;
    private int lcyCurrBalance;
    private int movementAmountTdy;

    public TXNAccount() {};

    public TXNAccount(int acy_lcy_balance, String branch_code, String cust_ac_no, String cust_no, String record_stat, int lcy_current_balance, int movement_amount_tdy) {
        this.acyLcyBalance = acy_lcy_balance;
        this.branchCode = branch_code;
        this.custAcNo = cust_ac_no;
        this.custNo = cust_no;
        this.recordStat = record_stat;
        this.lcyCurrBalance = lcy_current_balance;
        this.movementAmountTdy = movement_amount_tdy;
    }
}
