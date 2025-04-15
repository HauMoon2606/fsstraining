package DTO;
import lombok.Data;
@Data
public class Dbz_ODS_CRB_BAL {
    private ODS_CRB_BAL beforeData;
    private ODS_CRB_BAL afterData;
    private String op;

    public Dbz_ODS_CRB_BAL(){};
}
