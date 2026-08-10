package com.cgv.booking.web;

import com.cgv.booking.config.CgvProps;
import com.cgv.booking.service.AdminResetService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

// 운영용 초기화 API. 토큰(cgv.admin-token = env ADMIN_TOKEN)을 주입한 배포에만 존재한다 —
// 토큰이 없을 때 403을 돌려주는 방식이면 "이 API가 있다"는 사실 자체가 남는다.
// 파괴적 연산이라 인증은 서버에서만 판정한다(프론트의 ?ops 같은 화면 숨김은 잠금이 아니다).
@RestController
@RequestMapping("/api/admin")
@ConditionalOnExpression("'${cgv.admin-token:}' != ''")
public class AdminController {
    private final AdminResetService reset;
    private final String token;

    public AdminController(AdminResetService reset, CgvProps props) {
        this.reset = reset;
        this.token = props.getAdminToken();
    }

    @PostMapping("/reset")
    public Map<String, Object> reset(@RequestHeader(name = "X-Admin-Token", required = false) String given) {
        if (token.isEmpty() || !token.equals(given)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "forbidden");
        }
        return reset.resetAll();
    }
}
