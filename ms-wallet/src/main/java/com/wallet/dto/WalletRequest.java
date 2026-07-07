package com.wallet.dto;


public class WalletRequest {
	private String eventType; // CREDIT / DEBIT
    private String userId;
    private Double amount;
        
	public String getEventType() {
		return eventType;
	}
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public Double getAmount() {
		return amount;
	}
	public void setAmount(Double amount) {
		this.amount = amount;
	}

    // getters & setters
}