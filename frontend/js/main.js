// 부팅과 배선 — 이벤트 연결, 시계, 운영자 도구, 시작 시 상태 복구.

import {
  S, K, store, $, onScreen, feedLog, renderFeed, saveRoster, sweepExpiredGate,
} from './core.js';
import { startLivePolling } from './live.js';
import {
  renderAuth, login, loadMovies, enter, goHome, backFromSeats,
  toPay, confirmPay, openSeats, resetSession,
} from './booking.js';
import {
  renderStrip, addFakes, clearFakes, initConsole, adoptRoster, renderGate, fireGateEvents,
} from './sim.js';

// ================= 배선 =================
$('homeBtn').onclick = goHome;
$('waitLeave').onclick = goHome;
$('seatsBack').onclick = backFromSeats;
$('toPay').onclick = toPay;
$('payBack').onclick = () => openSeats(S.screeningId, S.screeningLabel, true);   // 잡아둔 좌석을 유지한 채 되돌아간다
$('payConfirm').onclick = confirmPay;
$('doneHome').onclick = () => { resetSession(); loadMovies(); };
$('heroAdd').onclick = () => addFakes(parseInt($('heroN').value));
$('clearFakes').onclick = clearFakes;
$('idHintBtn').onclick = login;
$('botMode').onchange = () => feedLog(
  $('botMode').checked
    ? '자동 예매 켜짐 — 입장한 관객이 예매하고 자리를 반환합니다'
    : '자동 예매 꺼짐 — 입장한 관객이 세션이 끝날 때까지 자리를 차지합니다', 'sys');

// 소개는 첫 방문에 펼쳐 보여준다(스토리·아키텍처·저장소 링크가 여기 있다). 접은 선택은 기억한다.
const introToggle = $('introToggle');
const setIntro = (open) => {
  $('introPanel').classList.toggle('hidden', !open);
  introToggle.setAttribute('aria-expanded', String(open));
  introToggle.textContent = open ? '이 시뮬레이터는? ▴' : '이 시뮬레이터는? ▾';
  store.set(K.INTRO_CLOSED, open ? '' : '1');
};
setIntro(!store.get(K.INTRO_CLOSED));
introToggle.onclick = () => setIntro($('introPanel').classList.contains('hidden'));

// ================= 운영자 도구 =================
// 주소의 ?ops는 폼을 여는 스위치일 뿐 잠금이 아니다. 판정은 서버가 토큰 헤더로 한다
// (토큰을 주입하지 않은 배포에는 그 API 자체가 없다).
function initOps() {
  if (!new URLSearchParams(location.search).has('ops')) return;
  $('opsBox').classList.remove('hidden');
  const msg = $('opsMsg');
  const btn = $('opsReset');

  btn.onclick = async () => {
    const token = $('opsToken').value.trim();
    if (!token) { msg.textContent = '토큰을 입력하세요.'; return; }
    if (!S.simMovieId) { msg.textContent = '상영 정보를 아직 불러오지 못했습니다 — 잠시 후 다시 시도하세요.'; return; }
    if (!confirm('예매 기록과 대기열을 전부 지웁니다. 계속할까요?')) return;

    btn.disabled = true;
    msg.textContent = '초기화 중…';
    const headers = { 'Content-Type': 'application/json', 'X-Admin-Token': token };
    // 대기열을 먼저 비운다 — 예매를 지우는 동안 새로 승격된 관객이 좌석을 잡아 잔재가 남지 않게.
    const q = await fetch('/api/admission/reset', { method: 'POST', headers, body: JSON.stringify({ movieId: S.simMovieId }) }).catch(() => null);
    const b = await fetch('/api/admin/reset', { method: 'POST', headers }).catch(() => null);
    btn.disabled = false;

    if (!q || !b || !q.ok || !b.ok) {
      const code = [q && q.status, b && b.status].filter(Boolean).join('/');
      msg.textContent = `초기화 실패 (${code || '연결 불가'}) — 토큰이나 서버 상태를 확인하세요.`;
      return;
    }
    const info = await b.json().catch(() => ({}));
    msg.textContent = `초기화 완료 — 예매 ${info.bookingsDeleted ?? 0}건 삭제, 방송일 ${(info.broadcastAt || '').slice(0, 16).replace('T', ' ')}`;
    clearFakes();
    feedLog('데이터 초기화 — 예매와 대기열을 비웠습니다', 'sys');
    loadMovies();
  };
}

// ================= 시계 =================
// 오픈 예약이 걸리면 이 시계가 기준이 된다. 대기 화면의 생존 신호 표시도 같은 틱에서 그린다.
function startClock() {
  const el = $('clock'); if (!el) return;
  const tick = () => {
    el.textContent = new Date().toTimeString().slice(0, 8);
    sweepExpiredGate();   // 표시 기간이 끝난 예약 청소
    renderGate();         // 카운트다운·버튼 잠금
    fireGateEvents();     // 오픈 러시·마감 처리(표식과 대조해 한 번만)
    const heart = $('waitHeart');
    if (heart && S.lastPollAt && onScreen('waiting')) {
      heart.textContent = `폴링 = 생존 신호 · 마지막 신호 ${Math.max(0, Math.round((Date.now() - S.lastPollAt) / 1000))}초 전`;
    }
  };
  tick();
  setInterval(tick, 1000);
}

// 페이지를 떠날 때 가상 관객 명단을 남긴다 — 다음에 여는 페이지가 이어받는다.
window.addEventListener('pagehide', saveRoster);

// ================= 시작 =================
(function boot() {
  const auth = store.getJSON(K.AUTH);
  if (auth && auth.rid) { S.rid = auth.rid; S.userLabel = auth.label; S.myRids.add(auth.rid); }
  renderAuth();

  initConsole();
  initOps();
  adoptRoster();     // 명단 이어받기는 목록을 그리기 전에 — 스트립이 처음부터 차 있게
  renderStrip();
  renderFeed();
  loadMovies();
  startClock();
  startLivePolling();

  // 새로고침 복구 — 대기·입장 중이었으면 같은 신원으로 다시 들어간다.
  // 서버는 입장 상태면 유지하고, 대기 중이었으면 꼬리로 민다.
  const flow = store.getJSON(K.FLOW);
  if (S.rid && flow && flow.movieId) {
    if (flow.activeUntil) S.resumeUntil = flow.activeUntil;
    enter(flow.movieId, flow.title);
  }
})();
