package Utils;

import DTO.BranchDim;
import DTO.CompanyBranchMap;

import java.sql.*;
import java.time.LocalDate;

public class DBUtils
{
    private static  String checkQuery = "SELECT * FROM public.dim_branch WHERE branch_code = ? ";
    private static String updateQuery = "UPDATE public.dim_branch SET sbv_effect_date = ?, parent_branch=?, sbv_code= ?," +
            "branch_name=?, cic_code =?, cif_id =?, end_dt =?, update_tms =?, act_f = 0 WHERE branch_code = ? AND act_f = 1";
    private static String insertQuery = "INSERT INTO public.dim_branch (BRANCH_CODE, SBV_EFFECT_DATE, PARENT_BRANCH, SBV_CODE, "
            + "BRANCH_NAME, CIC_CODE, CIF_ID, EFF_DT, END_DT, UPDATE_TMS, ACT_F) "
            + "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, 1)";

    public static  void addRecord(Connection conn, BranchDim newBranch){
        try(PreparedStatement insertStmt = conn.prepareStatement(insertQuery)){
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
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }
    public static void modifyOldRecord(Connection conn, BranchDim branchDim){
        try(PreparedStatement updateStmt= conn.prepareStatement(updateQuery)){
            updateStmt.setDate(1,branchDim.getSbv_effect_date());
            updateStmt.setString(2,branchDim.getParent_branch());
            updateStmt.setString(3,branchDim.getSbv_code());
            updateStmt.setString(4,branchDim.getBranch_name());
            updateStmt.setInt(5,branchDim.getCic_code());
            updateStmt.setInt(6,branchDim.getCif_id());
            updateStmt.setDate(7,Date.valueOf(LocalDate.now()));
            updateStmt.setTimestamp(8,new Timestamp(System.currentTimeMillis()));
            updateStmt.setString(9,branchDim.getBranch_code());
            updateStmt.executeUpdate();
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }
    public static void addRecord(Connection conn, CompanyBranchMap newBranchMap){
        try(PreparedStatement checkStmt= conn.prepareStatement(checkQuery)){
            checkStmt.setString(1,newBranchMap.getBranch_code());
            ResultSet rs = checkStmt.executeQuery();
            if(rs.next()){
                    BranchDim branchRecord = new BranchDim(rs.getString("branch_code"),newBranchMap.getSbv_effect_date(),
                            newBranchMap.getParent_branch(),newBranchMap.getSbv_code(),rs.getString("branch_name"),
                            rs.getInt("cic_code"),rs.getInt("cif_id"),rs.getDate("eff_dt"),
                            rs.getDate("end_dt"),rs.getTimestamp("update_tms"),rs.getInt("act_f"));
                    addRecord(conn,branchRecord);
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }

    }
    public static void modifyOldRecord(Connection conn, CompanyBranchMap newBranchMap){
        try(PreparedStatement checkStmt= conn.prepareStatement(checkQuery)){
            checkStmt.setString(1,newBranchMap.getBranch_code());
            ResultSet rs = checkStmt.executeQuery();
            if(rs.next()){
                BranchDim branchRecord = new BranchDim(rs.getString("branch_code"),rs.getDate("sbv_effect_date"),
                        rs.getString("parent_branch"),rs.getString("sbv_code"),rs.getString("branch_name"),
                        rs.getInt("cic_code"),rs.getInt("cif_id"),rs.getDate("eff_dt"),
                        rs.getDate("end_dt"),rs.getTimestamp("update_tms"),rs.getInt("act_f"));
                modifyOldRecord(conn, branchRecord);
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }
    public static void deleteRecord (Connection conn, CompanyBranchMap oldBranchMap){
        try(PreparedStatement checkStmt= conn.prepareStatement(checkQuery)){
            checkStmt.setString(1,oldBranchMap.getBranch_code());
            ResultSet rs = checkStmt.executeQuery();
            if(rs.next()){
                BranchDim branchRecord = new BranchDim(rs.getString("branch_code"),new Date(0),
                        "","",rs.getString("branch_name"),
                        rs.getInt("cic_code"),rs.getInt("cif_id"),rs.getDate("eff_dt"),
                        rs.getDate("end_dt"),rs.getTimestamp("update_tms"),rs.getInt("act_f"));

                modifyOldRecord(conn, oldBranchMap);
                addRecord(conn, branchRecord);
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }
}
