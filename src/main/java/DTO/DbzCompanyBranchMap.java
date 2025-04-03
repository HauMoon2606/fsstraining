package DTO;
import lombok.Data;
@Data
public class DbzCompanyBranchMap {
    private CompanyBranchMap beforeData;
    private CompanyBranchMap afterData;
    private String op;
    public DbzCompanyBranchMap(){};
}

