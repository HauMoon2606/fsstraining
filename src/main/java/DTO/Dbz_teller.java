package DTO;
import lombok.Data;
@Data
public class Dbz_teller {
    private Teller beforeData;
    private Teller afterData;
    private String op;

    public Dbz_teller(){};
}
