package DTO;
import lombok.Data;

import java.sql.Date;

@Data
public class Account {
    private String id;
    private String customer;
    private String prtCode;
    private String currency;
    private int openValDatedBal; // NUMBER
    private java.sql.Date dateMaturity; // DATE
    private String opType; // VARCHAR2(1)
    private int workingBalance; // NUMBER
    private String allInOneProduct; // VARCHAR2(50)
    private String category; // VARCHAR2(10)
    private String coCode; // VARCHAR2(10)

    public  Account(){}

    public Account(String id, String customer, String prtCode, String currency,
                   int openValDatedBal, Date dateMaturity, String opType,
                   int workingBalance, String allInOneProduct, String category, String coCode) {
        this.id = id;
        this.customer = customer;
        this.prtCode = prtCode;
        this.currency = currency;
        this.openValDatedBal = openValDatedBal;
        this.dateMaturity = dateMaturity;
        this.opType = opType;
        this.workingBalance = workingBalance;
        this.allInOneProduct = allInOneProduct;
        this.category = category;
        this.coCode = coCode;
    }
}
