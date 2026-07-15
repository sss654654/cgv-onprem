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
}
