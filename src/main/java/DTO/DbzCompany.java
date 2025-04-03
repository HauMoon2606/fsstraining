package DTO;
import lombok.Data;
@Data
public class DbzCompany {
    private Company beforeData;
    private Company afterData;
    private String op;
    public DbzCompany(){};
}
