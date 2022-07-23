package com.grainite.fundtransfer.bo;

public class FundTransferEvent {
  public String msgId;
  public String debitAccount;
  public String creditAccount;
  public String desc;
  public long amount;

  public FundTransferEvent() {}
}
