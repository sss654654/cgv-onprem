package com.cgv.booking.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

// application.yml의 cgv.* 도메인 픽스값(§3-1-8) 바인딩.
@Component
@ConfigurationProperties(prefix = "cgv")
public class CgvProps {
    private String movieId;
    private String movieTitle;
    private int pricePerSeat;
    private int seatLockTtlSeconds;
    private String branches;          // 콤마 구분 지점명
    private int screensPerBranch;
    private int rows;
    private int cols;
    private int admittedTtlSeconds;   // 입장 인증(admitted) 만료 — queue 세션 타임아웃보다 길어야 함
    private int maxSeatsPerRequest;   // 한 요청에 담을 수 있는 좌석 수 상한

    public String[] branchList() { return branches.split(","); }

    public String getMovieId() { return movieId; }
    public void setMovieId(String v) { this.movieId = v; }
    public String getMovieTitle() { return movieTitle; }
    public void setMovieTitle(String v) { this.movieTitle = v; }
    public int getPricePerSeat() { return pricePerSeat; }
    public void setPricePerSeat(int v) { this.pricePerSeat = v; }
    public int getSeatLockTtlSeconds() { return seatLockTtlSeconds; }
    public void setSeatLockTtlSeconds(int v) { this.seatLockTtlSeconds = v; }
    public String getBranches() { return branches; }
    public void setBranches(String v) { this.branches = v; }
    public int getScreensPerBranch() { return screensPerBranch; }
    public void setScreensPerBranch(int v) { this.screensPerBranch = v; }
    public int getRows() { return rows; }
    public void setRows(int v) { this.rows = v; }
    public int getCols() { return cols; }
    public void setCols(int v) { this.cols = v; }
    public int getAdmittedTtlSeconds() { return admittedTtlSeconds; }
    public void setAdmittedTtlSeconds(int v) { this.admittedTtlSeconds = v; }
    public int getMaxSeatsPerRequest() { return maxSeatsPerRequest; }
    public void setMaxSeatsPerRequest(int v) { this.maxSeatsPerRequest = v; }
}
