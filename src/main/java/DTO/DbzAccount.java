package DTO;
import lombok.Data;
@Data
public class DbzAccount {
    private Account beforeData;
    private Account afterData;
    private String op;

    public DbzAccount(){};
}
