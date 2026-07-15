package com.cgv.booking.web;

import com.cgv.booking.repo.MovieRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

// 영화(방송) 목록 — 프론트 첫 화면(영화목록). 게이트 없음(입장 *전* 진입점).
@RestController
@RequestMapping("/api/movies")
public class MovieController {
    private final MovieRepository movies;
    public MovieController(MovieRepository movies) { this.movies = movies; }

    public record MovieView(String id, String title, String broadcastAt) {}

    @GetMapping
    public List<MovieView> list() {
        return movies.findAll().stream()
                .map(m -> new MovieView(m.getId(), m.getTitle(),
                        m.getBroadcastAt() == null ? null : m.getBroadcastAt().toString()))
                .toList();
    }
}
