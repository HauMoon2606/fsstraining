package DTO;
import lombok.Data;
@Data
public class ODS_CRB_BAL {
    private String id;
    private int lcy_bal;
    private String arrangement_nbr;
    private int coa_code;

    public ODS_CRB_BAL(){};

    public ODS_CRB_BAL(String id, int lcy_bal, String arrangement_nbr, int coa_code) {
        this.id = id;
        this.lcy_bal = lcy_bal;
        this.arrangement_nbr = arrangement_nbr;
        this.coa_code = coa_code;
    }

    public ODS_CRB_BAL(int lcy_bal, String arrangement_nbr, int coa_code){
        this.lcy_bal = lcy_bal;
        this.arrangement_nbr = arrangement_nbr;
        this.coa_code = coa_code;
    }
}
