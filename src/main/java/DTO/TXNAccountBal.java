package DTO;
import lombok.Data;
@Data
public class TXNAccountBal {
    private int acyLcyBalance;
    private String branchCode;
    private String custAcNo;
    private String custNo;
    private int lcyBalance;
    private String recordStat;
    private int lcyCurrBalance;
    private int movementAmountTdy;

    public TXNAccountBal() {};

    public TXNAccountBal(int acy_lcy_balance, String branch_code, String cust_ac_no,String cust_no, int lcy_balance, String record_stat,int lcy_current_balance, int movement_amount_tdy) {
        this.acyLcyBalance = acy_lcy_balance;
        this.branchCode = branch_code;
        this.custAcNo = cust_ac_no;
        this.custNo = cust_no;
        this.lcyBalance = lcy_balance;
        this.recordStat = record_stat;
        this.lcyCurrBalance = lcy_current_balance;
        this.movementAmountTdy = movement_amount_tdy;
    }
}
