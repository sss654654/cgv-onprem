// 부팅과 배선 — 이벤트 연결, 시계, 시작 시 상태 복구.

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

// 소개는 접은 채로 시작한다. 펼쳐 두면 인용구·아키텍처·주의문이 첫 화면을 차지해
//   실제로 만질 것(예매 카드와 콘솔)이 스크롤 아래로 밀린다. 무엇인지는 부제 한 줄이 말하고,
//   더 볼 사람만 연다. 편 선택은 기억한다.
const introToggle = $('introToggle');
const setIntro = (open) => {
  $('introPanel').classList.toggle('hidden', !open);
  introToggle.setAttribute('aria-expanded', String(open));
  introToggle.textContent = open ? '이 시뮬레이터는? ▴' : '이 시뮬레이터는? ▾';
  store.set(K.INTRO_OPEN, open ? '1' : '');
};
setIntro(!!store.get(K.INTRO_OPEN));
introToggle.onclick = () => setIntro($('introPanel').classList.contains('hidden'));

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
