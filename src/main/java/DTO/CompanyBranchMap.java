package DTO;

import lombok.Data;

import java.sql.*;
import java.time.LocalDate;


@Data
public class CompanyBranchMap  {

    private String branch_code;
    private String sbv_code;
    private Date sbv_effect_date;
    private String parent_branch;

    public CompanyBranchMap() {}
    public CompanyBranchMap(String branch_code, String sbv_code,Date sbv_effect_date, String PARENT_BRANCH ){
        this.branch_code = branch_code;
        this.sbv_code = sbv_code;
        this.sbv_effect_date = sbv_effect_date;
        this.parent_branch = PARENT_BRANCH;
    }
    public static CompanyBranchMap getPostgresRecord(String key){
        String postgres_url = "jdbc:postgresql://localhost:5432/k6";
        String postgres_username = "k6";
        String postgres_password = "k6";
        String source_table = "public.cic_company_branch_map";
        try(Connection conn = DriverManager.getConnection(postgres_url,postgres_username,postgres_password)){
            String query = "SELECT * FROM " + source_table + " WHERE branch_code = ?";
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setString(1,key);
            ResultSet rs = ps.executeQuery();
            if(rs.next()){
                return new CompanyBranchMap(
                        rs.getString("branch_code"),
                        rs.getString("sbv_code"),
                        rs.getDate("sbv_effect_date"),
                        rs.getString("parent_branch"));
            }

        }catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }

}
