package org.example.nem.constant;

public enum ErrorType {
    Invalid_RowType(1, "invalid row type"),
    Freq_Mismatch(2, "frequency mismatch"),
    NMI_Missing(3, "nmi base info missing"),
    Invalid_NMI_Info(4, "invalid nmi info"),
    Invalid_Consumption_Value(5, "invalid consumption value"),
    No_Consumption_Data(6, "no consumption data"),

    Parse_Error(98, "parse error"),
    Unknown_Error(99, "unknown error");
    private final int code;
    private final String message;

    ErrorType(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

}
