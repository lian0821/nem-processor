package org.example.nem.data;

public class RuntimeState {
    public String nmi = "";
    public int interval = 0;
    public String inputFileName = "";
    public long currentLineCnt = 0;

    public RuntimeState() {
    }

    public RuntimeState(String nmi, int interval, String inputFileName, long currentLineCnt) {
        this.nmi = nmi;
        this.interval = interval;
        this.inputFileName = inputFileName;
        this.currentLineCnt = currentLineCnt;
    }
}
