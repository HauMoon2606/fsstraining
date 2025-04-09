package DTO;
import Utils.Utils;
import lombok.Data;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

@Data
public class Company {
    private String id;
    private int memonic_lp ;
    private String company_name;
    private int cif_id;
    private int cic;
    private String branch_code;
    private String branch_name;
    private String parent_id;
    private int working_balance;
    public Company() {};
    public BranchDim joinBranchMap(){
        String key = Utils.NVL(Utils.LPAD(String.valueOf(this.memonic_lp),3,'0'),this.id);
        CompanyBranchMap postgresRecord = CompanyBranchMap.getPostgresRecord(key);
        if(postgresRecord!= null){
            Date sbvEffectDate = postgresRecord.getSbv_effect_date();
            String parentBranch = postgresRecord.getParent_branch();
            String sbvCode = postgresRecord.getSbv_code();
            String branchName = this.company_name;
            int cicCode = Integer.parseInt(Utils.NVL(String.valueOf(this.cic),postgresRecord.getSbv_code()));
            int cifId = this.cif_id;
            return new BranchDim(key,sbvEffectDate,parentBranch,sbvCode,branchName,cicCode,cifId,Date.valueOf(LocalDate.now()),new Date(0),new Timestamp(0),0);
        }
        else{
            return new BranchDim(key,new Date(0),"","",this.getCompany_name(),0,0,Date.valueOf(LocalDate.now()),new Date(0),new Timestamp(0),0);
        }

    }

}
