package DTO;
import lombok.Data;

import java.sql.*;
import java.time.LocalDate;

@Data
public class BranchDim {
    private String branch_code;
    private Date sbv_effect_date;
    private String parent_branch;
    private String sbv_code;
    private String branch_name;
    private int cic_code;
    private int cif_id;
    private Date eff_dt;
    private Date end_dt;
    private Timestamp update_tms;
    private int act_f;
    public BranchDim(String branch_code, Date sbv_effect_date, String parent_branch, String sbv_code,
                     String branch_name, int cic_code, int cif_id, Date eff_dt, Date end_dt,
                     Timestamp update_tms, int act_f) {
        this.branch_code = branch_code;
        this.sbv_effect_date = sbv_effect_date;
        this.parent_branch = parent_branch;
        this.sbv_code = sbv_code;
        this.branch_name = branch_name;
        this.cic_code = cic_code;
        this.cif_id = cif_id;
        this.eff_dt = eff_dt;
        this.end_dt = end_dt;
        this.update_tms = update_tms;
        this.act_f = act_f;
    }
    public BranchDim(){};
    public static void  updateRecordInDay(Connection conn, BranchDim newBranch) throws SQLException{
        String query = "UPDATE public.dim_branch set sbv_effect_date =?, parent_branch=?, sbv_code =?, "+
        "branch_name=?, cic_code =?, cif_id=?, eff_dt=?, end_dt=?, update_tms=?, act_f=? where branch_code=?"+
                " and act_f = 1";
        PreparedStatement ps = conn.prepareStatement(query);
        ps.setDate(1, newBranch.sbv_effect_date);
        ps.setString(2, newBranch.parent_branch);
        ps.setString(3, newBranch.sbv_code);
        ps.setString(4, newBranch.branch_name);
        ps.setInt(5, newBranch.cic_code);
        ps.setInt(6, newBranch.cif_id);
        ps.setDate(7, newBranch.eff_dt);
        ps.setDate(8, null);
        ps.setDate(9, Date.valueOf(LocalDate.now()));
        ps.setInt(10, 1);
        ps.setString(11, newBranch.branch_code);
        ps.executeUpdate();
    }
    public void addOrUpdate(Connection conn,BranchDim newBranch) throws SQLException {
        String checkQuery = "SELECT * FROM public.dim_branch WHERE branch_code = ? AND act_f = 1";
        String updateQuery = "UPDATE public.dim_branch SET end_dt = ?, act_f = 0 WHERE branch_code = ? AND act_f = 1";
        String insertQuery = "INSERT INTO public.dim_branch (BRANCH_CODE, SBV_EFFECT_DATE, PARENT_BRANCH, SBV_CODE, "
                + "BRANCH_NAME, CIC_CODE, CIF_ID, EFF_DT, END_DT, UPDATE_TMS, ACT_F) "
                + "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, 1)";

        try (PreparedStatement checkStmt = conn.prepareStatement(checkQuery);
             PreparedStatement updateStmt = conn.prepareStatement(updateQuery);
             PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {

            checkStmt.setString(1, newBranch.getBranch_code());
            ResultSet rs = checkStmt.executeQuery();

            if (rs.next()) {
                boolean isChanged = !rs.getString("BRANCH_NAME").equals(newBranch.getBranch_name()) ||
                        !rs.getString("PARENT_BRANCH").equals(newBranch.getParent_branch()) ||
                        !rs.getString("SBV_CODE").equals(newBranch.getSbv_code()) ||
                        rs.getInt("CIC_CODE") != newBranch.getCic_code() ||
                        rs.getInt("CIF_ID") != newBranch.getCif_id();
                if (!isChanged) {
                    System.out.println("No changes detected, skipping insert.");
                    return;
                }
                // nếu ngày thay đổi = ngày hiện tại thì sửa trực tiếp
                if (newBranch.eff_dt == rs.getDate("eff_dt")){
                    BranchDim.updateRecordInDay(conn, newBranch);
                    return;
                }
                updateStmt.setDate(1, Date.valueOf(LocalDate.now()));
                updateStmt.setString(2, newBranch.getBranch_code());
                updateStmt.executeUpdate();
            }
            insertStmt.setString(1, newBranch.getBranch_code());
            insertStmt.setDate(2, newBranch.getSbv_effect_date());
            insertStmt.setString(3, newBranch.getParent_branch());
            insertStmt.setString(4, newBranch.getSbv_code());
            insertStmt.setString(5, newBranch.getBranch_name());
            insertStmt.setInt(6, newBranch.getCic_code());
            insertStmt.setInt(7, newBranch.getCif_id());
            insertStmt.setDate(8, Date.valueOf(LocalDate.now())); // EFF_DT
            insertStmt.setTimestamp(9, new Timestamp(System.currentTimeMillis())); // UPDATE_TMS
            insertStmt.executeUpdate();
            System.out.println("Branch record updated successfully.");
        }
    }
}
