package Utils;

public class Utils {
    public static String NVL (String valueNull, String valueDefault) {
        if(valueNull != null){
            return LPAD(valueNull,3,'0');
        }
        return valueDefault;
    }

    public static String LPAD(String value, int length, char additionChar) {
        int padLength = length - value.length();
        if (padLength <= 0) {
            return value;
        }
        StringBuilder padding = new StringBuilder();
        for (int i = 0; i < padLength; i++) {
            padding.append(additionChar);
        }
        return padding.append(value).toString();
    }
}
