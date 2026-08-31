// 시뮬레이터 — 가상 관객 엔진과 오픈 게이트.
// 가상 관객은 가짜 requestId로 대기열에 들어간 클라이언트다. 상태 전이(대기→입장→예매→퇴장)는
// 3초 틱의 폴링이 관찰하고, 스트립의 점과 실황이 그 관찰을 그대로 그린다.
// 폴링이 곧 생존 신호라, 틱이 멈추면 대기 중인 관객은 30초 만에 서버가 회수한다.

import {
  S, K, store, $, toast, api, uuid, sleep, feedLog, fmtAt, fmtHMS, sessionSecs,
  MAX_FAKES, INJECT_PRESETS, RUSH_PRESETS,
  effectiveGate, openAtMs, openRushN, openFiredAt, markOpenFired, clearOpen, isRushFresh,
  claimOpenFire, wonOpenFire,
  activeFakes, lobbyFakes, releaseSeats, leaveQueue, releaseSlot, takeRoster, onScreen,
} from './core.js';
import { goHome } from './booking.js';

// 병렬 폭. 6은 HTTP/1.1의 "동일 오리진 동시연결 6개" 전제에서 나온 값인데,
//   공개 경로는 HTTP/2라 연결 하나에서 요청이 겹친다 — 6으로 묶을 이유가 없다.
// 이 폭이 좁으면 왕복 시간이 그대로 곱해진다. 실측(2026-08-28): 같은 API가
//   LAN 직결 약 0.037초, 공개 경로 약 0.85초(Cloudflare 거점이 LAX로 잡힌다).
//   50명을 6명씩 훑으면 폴링만 9회 × 0.85초 ≈ 7.6초로 틱(3초)을 넘겨,
//   자동 예매에 쓸 시간이 남지 않아 세션 수명(60초) 안에 무리를 못 비운다.
const CHUNK = 24;       // 생존 폴링
const TICK_MS = 3000;   // 가상 관객 폴링 주기
// 예매는 한 건이 네 번 순차 왕복(회차→좌석→선점→확정)이라 폭을 더 준다.
const BOOK_CHUNK = 24;
// 한 틱에서 자동 예매에 쓸 수 있는 시간. 나머지는 생존 폴링 몫으로 남긴다 —
//   폴링이 끊기면 서버가 대기자를 회수하므로, 예매보다 폴링이 먼저다.
const BOOK_BUDGET_MS = 1800;

// 사람처럼 시간을 쓰게 하는 랜덤 둘 — 데모 가시성 장치다.
//   투입 지터: 봇마다 0-4초 흩어져 줄을 선다. 흩지 않으면 정원(20)만큼이 같은 틱에
//     들어가고, 이후의 예매·퇴장·재승격까지 전부 그 박자로 묶여 순번이 20씩 계단으로
//     떨어진다(2026-08-31 데모 관찰).
//   예매 체류: 승격 후 6-18초 뒤에 예매한다 — 회차·좌석을 고르는 시간. 같이 입장한
//     무리가 같은 순간 빠져나가는 것(좌석 동시 감소·자리 동시 반환)이 사라진다.
//     세션 수명(서버 값, 지금 300초)보다 훨씬 짧아 만료 위험은 없다.
const ENTER_JITTER_MS = 4000;
const bookDelay = () => 6000 + Math.random() * 12000;

// ================= 스트립 =================
const DOT_CLASS = { lobby: 'd-lobby', waiting: 'd-wait', admitted: 'd-in', booked: 'd-book', gone: 'd-gone' };
const DOT_NAME = {
  lobby: '대기실 — 오픈을 기다립니다',
  waiting: '대기 중',
  admitted: '입장 — 정원 안입니다',
  booked: '예매 성공 — 자리를 반환하고 나갔습니다',
  gone: '빈손 퇴장 — 아무것도 사지 못하고 나갔습니다',
};

// 끝난 관객(예매 성공·빈손 퇴장)은 스트립에 흔적으로 남긴다 — 한 판이 어떻게 끝났는지가
//   그 점들이다. 다만 다음 판이 그 위에 겹치면 두 판이 한 줄에 섞여 읽히지 않는다.
//   그래서 "새 판을 시작한다"는 행동(투입·오픈 러시)에서 앞 판의 흔적을 비운다.
//   시간으로 지우지 않는 이유: 보고 있는 도중에 사라진다. 판이 끝나자마자 지우지 않는 이유:
//   결과를 볼 새가 없다. 다음 판을 시작하겠다는 것이 곧 앞 판을 다 봤다는 신호다.
export function clearDone() {
  const before = S.fakes.length;
  S.fakes = S.fakes.filter(f => f.state !== 'booked' && f.state !== 'gone');
  return before - S.fakes.length;
}

// 무한히 쌓이는 것은 위 초기화와 별개로 막는다 — 한 판 안에서도 회전이 계속되면 늘어난다.
// 여럿 중 하나를 고른다. 목록 순서대로 고르면 같은 틱의 관객 전원이 같은 것을 집는다.
const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];

const KEEP_DONE = 200;
function pruneDone() {
  const done = S.fakes.filter(f => f.state === 'booked' || f.state === 'gone');
  if (done.length <= KEEP_DONE) return;
  const drop = new Set(done.slice(0, done.length - KEEP_DONE));
  S.fakes = S.fakes.filter(f => !drop.has(f));
}

// 범례는 지금 화면에 있는 상태만 싣는다. 다섯 개를 늘 펼쳐 두면 관객이 없을 때도
//   설명만 남고, 대기실처럼 오픈 예약이 걸렸을 때만 생기는 상태까지 상시 노출된다.
const LEGEND = [
  ['lobby', '대기실'], ['waiting', '대기'], ['admitted', '입장'],
  ['booked', '예매 성공'], ['gone', '빈손 퇴장'],
];
function renderLegend() {
  const el = $('qvLegend'); if (!el) return;
  const have = new Set(S.fakes.map(f => f.state));
  el.innerHTML = LEGEND.filter(([s]) => have.has(s))
    .map(([s, name]) => `<span class="lg-grp"><i class="d ${DOT_CLASS[s]}"></i>${name}</span>`).join('');
}

export function renderStrip() {
  const box = $('queueStrip'); if (!box) return;
  pruneDone();
  box.innerHTML = S.fakes.length
    ? S.fakes.map(f => `<i class="d ${DOT_CLASS[f.state]}" title="관객-${f.label} · ${DOT_NAME[f.state]}"></i>`).join('')
    : '<span class="qv-empty">아직 가상 관객이 없습니다 — 위에서 넣어보세요</span>';
  $('qvCount').textContent = activeFakes().length;
  renderLegend();
}

// ================= 관객 생성·투입 =================
// 생성만 하고 아직 서버로 보내지 않는다(대기실에 모으는 경우가 있다).
// 새로 만든다는 것이 곧 새 판이라, 앞 판의 끝난 흔적을 여기서 비운다.
function createFakes(n, state) {
  clearDone();
  const list = [];
  for (let i = 0; i < n; i++) {
    const rid = 'bot-' + uuid().slice(0, 8);
    const f = { rid, label: rid.slice(4), state, entered: false };
    S.fakes.push(f); list.push(f); S.myRids.add(rid);
  }
  return list;
}

// 실제로 줄을 세운다. 봇마다 0-4초 지터를 주고 각자 줄을 선다 — 동시 입장 뭉텅이 방지.
// 청크가 필요 없다: 4초에 최대 100명이면 동시 진행 요청이 왕복(공개 경로 약 0.85초) 기준
//   20여 개라 생존 창(30초)에 한참 못 미친다.
async function enterFakes(list, label) {
  if (!list.length) return;
  const gen = S.simGen;
  const btn = $('heroAdd');
  const btnText = btn.textContent;
  btn.disabled = true;
  if (!S.simTimer) S.simTimer = setInterval(botTick, TICK_MS);   // 투입 중에도 생존 폴링이 돌게 미리 켠다

  let admitted = 0, waiting = 0, failed = 0, sent = 0;
  feedLog(`${label} ${list.length}명이 줄을 섭니다`, 'sys');
  try {
    await Promise.all(list.map(f =>
      sleep(Math.random() * ENTER_JITTER_MS).then(() => {
        if (gen !== S.simGen) return;
        return api('/api/admission/enter', { method: 'POST', body: { movieId: S.simMovieId, requestId: f.rid } })
          .then(({ data }) => {
            if (gen !== S.simGen) return;
            if (data && (data.status === 'ADMITTED' || data.status === 'ALREADY_ACTIVE')) {
              f.state = 'admitted'; f.entered = true; f.bookAt = Date.now() + bookDelay(); admitted++;
            }
            else if (data && data.status === 'WAITING') { f.state = 'waiting'; f.entered = true; waiting++; }
            else { f.state = 'gone'; failed++; }   // 지연·네트워크 실패를 입장으로 위장하지 않는다
            sent++;
            btn.textContent = `투입 중 ${sent}/${list.length}`;
            if (sent % 5 === 0 || sent === list.length) renderStrip();
          });
      })
    ));
  } finally { btn.disabled = false; btn.textContent = btnText; }

  if (gen !== S.simGen) return;
  feedLog(`줄 서기 완료 — 즉시 입장 ${admitted} · 대기 ${waiting}${failed ? ` · 실패 ${failed}` : ''}`, failed ? 'gone' : 'sys');
  renderStrip();
}

export async function addFakes(n) {
  if (addFakes._busy) return;
  if (!S.simMovieId) { toast('상영 정보를 먼저 불러오세요.', true); return; }
  n = Math.max(1, Math.min(MAX_FAKES, n || 5));
  const live = activeFakes().length;
  const gs = effectiveGate().s;
  // 예약 러시는 아직 화면에 없지만 오픈 순간 진입할 예정 인원이다 — 예산에 미리 넣는다.
  const reserved = gs === 'before' ? openRushN() : 0;
  if (live + reserved + n > MAX_FAKES) {
    toast(`동시 최대 ${MAX_FAKES}명까지입니다 — 지금 ${live}명${reserved ? ` + 예약 러시 ${reserved}명` : ''}`, true);
    return;
  }
  if (gs === 'before') {
    // 오픈 전에는 줄을 세우지 않는다. 미리 넣은 관객이 앞자리를 차지하면 오픈 시각에 들어오는
    // 사람이 이미 뒤처진 채로 시작한다. 대기실에 모아 두었다가 오픈 순간에 함께 내보낸다.
    createFakes(n, 'lobby');
    feedLog(`관객 ${n}명이 대기실에 들어왔습니다 — 오픈 시각에 함께 줄을 섭니다`, 'sys');
    renderStrip(); renderGate();
    return;
  }
  if (gs === 'closed') { toast('예매가 마감돼 관객을 넣을 수 없습니다.', true); return; }

  addFakes._busy = true;
  try { await enterFakes(createFakes(n, 'waiting'), '가상 관객'); }
  finally { addFakes._busy = false; }
}

// 대기실 개방 — 모아 둔 관객과 오픈 러시를 같은 순간에 내보낸다.
async function releaseLobby(rushN) {
  if (addFakes._busy) return;
  const lobby = lobbyFakes();
  // 러시 인원은 전체 활성 기준으로 자른다 — 대기실만 빼고 계산하면 기존 대기자와 합쳐 상한을 넘는다.
  // 투입 시점에 예약분까지 예산 검사를 하므로 보통 여기서 잘릴 일이 없다.
  //   그래도 잘리는 경우(두 창 병행 등)는 조용히 줄이지 않고 실황에 남긴다.
  const room = Math.max(0, MAX_FAKES - activeFakes().length);
  const wantRush = rushN || 0;
  const rush = createFakes(Math.min(wantRush, room), 'waiting');
  if (rush.length < wantRush) {
    feedLog(`동시 상한 ${MAX_FAKES}명 — 러시 ${wantRush}명 중 ${rush.length}명만 줄을 섭니다`, 'sys');
  }
  if (!lobby.length && !rush.length) return;
  addFakes._busy = true;
  try { await enterFakes([...lobby, ...rush], '오픈 러시'); }
  finally { addFakes._busy = false; }
}

// 마감 처리 — 줄에 남아 있던 관객을 전부 내보낸다.
async function kickWaiting() {
  const stuck = S.fakes.filter(f => f.state === 'waiting' || f.state === 'lobby');
  stuck.forEach(f => {
    if (f.entered) leaveQueue(S.simMovieId, f.rid);
    f.state = 'gone';
  });
  if (stuck.length) feedLog(`마감 — 대기 중이던 ${stuck.length}명이 퇴장했습니다`, 'gone');
  renderStrip();
  // 사람도 대기 중이었다면 함께 나온다. 이미 입장한 뒤라면 세션은 그대로 둔다(들어간 사람은 마저 예매한다).
  if (onScreen('waiting')) {
    toast('예매가 마감되어 대기열에서 나왔습니다.', true);
    await goHome();
  }
}

// ================= 관객 틱 =================
// ① 대기 관객: 폴링이 곧 생존 도장이라 이 틱이 줄을 유지시킨다.
// ② 입장 관객: 자동 예매가 켜져 있으면 좌석을 잡고 예매하고 나간다. 꺼져 있으면 세션이 끝날 때까지 자리를 쓴다.
export async function botTick() {
  if (botTick._busy) return;   // 재진입 가드 — 관객이 많으면 틱이 겹쳐 같은 요청이 중복된다
  if (!S.simMovieId) return;   // 영화 목록을 아직 못 받았으면 다음 틱을 기다린다
  botTick._busy = true;
  const gen = S.simGen;
  const gs = effectiveGate().s;
  // 오픈 전·마감 후에는 자동 예매를 쉰다 — 오픈 전에 줄만 쌓이는 것이 실제 티케팅과 같다.
  const canBook = $('botMode').checked && (gs === 'none' || gs === 'open');

  try {
    // ① 생존 폴링 — 순차로 돌면 관객 수 × 왕복 시간이 생존 창을 넘는다
    const live = S.fakes.filter(f => f.entered && f.state !== 'booked' && f.state !== 'gone');
    // 만료는 한 명씩 흩어져 오지 않는다. 함께 입장한 무리는 세션 수명이 같은 시각에 끝나
    //   서버가 한 번에 회수하고, 그 결과가 이 틱에서 한꺼번에 관찰된다.
    //   한 줄씩 찍으면 실황이 수십 줄의 퇴장으로 덮여 무언가 무너진 것처럼 보인다 — 한 줄로 묶는다.
    const expired = [];
    for (let i = 0; i < live.length && gen === S.simGen; i += CHUNK) {
      await Promise.all(live.slice(i, i + CHUNK).map(async f => {
        const p = await api(`/api/admission/position?movieId=${encodeURIComponent(S.simMovieId)}&requestId=${encodeURIComponent(f.rid)}`);
        if (gen !== S.simGen || !p.ok || !p.data) return;
        if (p.data.status === 'EXPIRED') { f.state = 'gone'; expired.push(f.label); return; }
        if (p.data.status !== 'ADMITTED') return;   // 대기 상태면 도장만 찍고 줄을 유지한다
        if (f.state === 'waiting') {
          f.state = 'admitted';
          f.bookAt = Date.now() + bookDelay();   // 승격 직후 바로 예매하지 않는다 — 고르는 시간
          feedLog(`관객-${f.label} 승격 → 입장`, 'in');
        }
      }));
    }
    if (expired.length === 1) feedLog(`관객-${expired[0]} 퇴장 — 세션 만료`, 'gone');
    else if (expired.length > 1) {
      feedLog(`관객 ${expired.length}명 동시 퇴장 — 입장 후 ${sessionSecs()}초 안에 예매하지 못해 자리가 회수됐습니다`, 'gone');
    }

    // ② 자동 예매 여정.
    // 한 명이 네 번 왕복하는데(회차·좌석·선점·예매) 이것을 전원에 대해 순차로 돌면
    //   입장자가 많을 때 틱 하나가 수십 초가 된다. 그동안 ①이 못 돌고, 폴링이 곧 생존
    //   도장이라 서버가 대기자를 전원 회수한다 — 예매를 시키려다 줄을 지우는 셈이다.
    // 그래서 둘을 건다: 청크 단위 병렬로 왕복을 겹치고, 남은 인원은 다음 틱으로 미룬다.
    //   미뤄도 손해가 없다 — 입장 자격은 세션이 살아 있는 동안 유지된다.
    const bookDeadline = Date.now() + BOOK_BUDGET_MS;
    if (canBook) {
      // 아직 "고르는 중"(bookAt 전)인 관객은 이번 틱을 건너뛴다 — 다음 틱에 잡힌다.
      const now = Date.now();
      const ready = S.fakes.filter(f => f.state === 'admitted' && (f.bookAt || 0) <= now);
      for (let i = 0; i < ready.length && gen === S.simGen; i += BOOK_CHUNK) {
        if (Date.now() > bookDeadline) break;   // 예산을 넘기면 여기서 끊고 ①을 지킨다
        await Promise.all(ready.slice(i, i + BOOK_CHUNK).map(async f => {
        if (gen !== S.simGen) return;
        if (f.state !== 'admitted') return;

        const sc = await api(`/api/screenings?movieId=${encodeURIComponent(S.simMovieId)}&requestId=${encodeURIComponent(f.rid)}`);
        if (!sc.ok || !sc.data) return;   // 입장 직후 인증이 아직 없으면 403 — 다음 틱에 다시 시도한다
        // 회차를 목록 순서대로 고르면(첫 번째 여유 회차) 모든 관객이 같은 관으로 몰린다.
        //   화면에서는 한 회차만 줄고 나머지는 그대로라 판이 실제와 다르게 보이고,
        //   좌석 선점이 그 회차 하나에 집중돼 서로 부딪힌다. 여유 있는 회차 중에서 흩는다.
        const opens = sc.data.filter(s => s.remain > 0);
        if (!opens.length) { f.state = 'gone'; feedLog(`관객-${f.label} 퇴장 — 전 회차 매진`, 'gone'); return; }
        const avail = pick(opens);

        const seats = await api(`/api/seats?screeningId=${encodeURIComponent(avail.screeningId)}&requestId=${encodeURIComponent(f.rid)}`);
        // 좌석도 같은 이유로 흩는다. 첫 빈 좌석을 고르면 같은 틱의 관객 전원이 같은 좌석을
        //   선점하려 해 하나만 성공하고 나머지는 409로 돌아간다 — 병렬 폭을 올려도
        //   실제 처리량이 틱당 한두 명에 머무는 원인이 이것이다.
        const frees = (seats.data || []).filter(s => !s.taken);
        if (!frees.length) return;
        const free = pick(frees);

        const sel = await api('/api/seats/select', { method: 'POST', body: { screeningId: avail.screeningId, seatNos: [free.seatNo], requestId: f.rid } });
        if (!sel.ok) return;

        const bk = await api('/api/bookings', { method: 'POST', body: { screeningId: avail.screeningId, seatNos: [free.seatNo], requestId: f.rid, idempotencyKey: `${f.rid}:${avail.screeningId}:${free.seatNo}` } });
        if (bk.ok) {
          f.state = 'booked';
          feedLog(`관객-${f.label} ${avail.branch} ${avail.screenNo}관 ${free.seatNo} 예매 완료 → 자리 반환`, 'book');
        } else {
          // 실패하면 잡은 좌석과 자리를 돌려주고 끝낸다. 안 그러면 만료까지 점유가 남는다.
          await releaseSeats(avail.screeningId, [free.seatNo], f.rid);
          await releaseSlot(S.simMovieId, f.rid, true);   // 입장 상태에서 반환이라 complete
          f.state = 'gone';
          feedLog(`관객-${f.label} 예매 실패 → 퇴장`, 'gone');
        }
        }));
      }
    }
    renderStrip();
  } finally { botTick._busy = false; }
}

// 초기화 — 한 판을 끝내고 새 판을 시작한다. 줄에 남은 관객은 서버에서 빼고, 끝난 관객은 목록에서 지운다.
// 서버 실황은 건드리지 않는다(내 것이 아니라 전역 기록이고, 지워도 다음 폴링에 다시 채워진다).
export async function clearFakes() {
  S.simGen++;   // 진행 중인 투입·틱을 즉시 무효화한다
  let live = 0;
  const total = S.fakes.length;
  for (const f of S.fakes) {
    if (f.state === 'lobby') { live++; continue; }   // 서버에 없던 관객이라 목록에서 빼면 끝이다
    if (f.entered && (f.state === 'waiting' || f.state === 'admitted')) {
      live++; releaseSlot(S.simMovieId, f.rid, f.state === 'admitted');
    }
  }
  S.fakes = [];
  clearInterval(S.simTimer); S.simTimer = null;
  store.del(K.ROSTER);   // 이어받을 명단도 함께 — 초기화는 새로고침 뒤에도 유효해야 한다
  const done = total - live;
  feedLog(`가상 관객 초기화 — 남은 ${live}명을 대기열에서 뺐습니다${done ? ` · 끝난 기록 ${done}건을 지웠습니다` : ''}`, 'sys');
  renderStrip();
}

// ================= 오픈 게이트 =================
// 그리기와 발동을 나눈다. 그리기는 매초 불려도 안전한 순수 표시고, 발동은 상태가 바뀌는 순간
// 한 번만 일어나야 하는 일(러시 투입·마감 처리)이다.

export function renderGate() {
  const st = effectiveGate();

  // 예매 버튼과, 오픈 전에만 나오는 탈출구
  document.querySelectorAll('.go').forEach(el => {
    el.classList.remove('locked', 'closed');
    if (st.s === 'before') { el.classList.add('locked'); el.textContent = `오픈까지 ${fmtHMS(st.remain)}`; el.disabled = true; }
    else if (st.s === 'closed') { el.classList.add('closed'); el.textContent = '예매 마감'; el.disabled = true; }
    else { el.textContent = '예매하기 →'; el.disabled = false; }
  });

  // 콘솔의 예약 줄은 예약 "내용"만 말한다. 진행 상태와 카운트다운은 시계 블록이 단일 출처다.
  const note = $('openNote');
  if (note) {
    if (st.s === 'before') {
      const lobbyN = lobbyFakes().length;
      const total = openRushN() + lobbyN;
      // 대기실·러시는 이 브라우저가 오픈 순간에 내보낼 인원이다 — 다른 방문자의 화면에는 없다.
      note.textContent = `다음 오픈 ${fmtAt(openAtMs())}`
        + (total > 0 ? ` · 이 브라우저에서 ${total}명 동시 진입(대기실 ${lobbyN} + 러시 ${openRushN()})` : '');
    } else if (st.s === 'open') note.textContent = `진행 중 오픈 ${fmtAt(openAtMs())} — 마감 후 다시 예약할 수 있습니다`;
    else if (st.s === 'closed') note.textContent = '지난 오픈이 마감됐습니다 — 해제 후 다시 예약하세요';
    else note.textContent = '';
  }

  // 진행 중에는 예약 변경을 잠근다 — 진행 중이던 오픈이 안내 없이 취소되지 않게.
  const setBtn = $('openSet');
  if (setBtn) setBtn.disabled = (st.s === 'open');

  // 시계 블록 — 상태의 단일 출처. 오픈이 다가올수록 색이 올라간다.
  const box = document.querySelector('.con-clock');
  const sub = $('clockSub');
  if (!box) return;
  box.classList.remove('warn', 'hot', 'live');
  let subText = '';
  if (st.s === 'before') {
    if (st.remain <= 10000) box.classList.add('hot');
    else if (st.remain <= 60000) box.classList.add('warn');
    subText = `오픈까지 ${fmtHMS(st.remain)}`;
  } else if (st.s === 'open') {
    box.classList.add('live');
    subText = `● 예매 진행 중 · 마감까지 ${fmtHMS(st.remain)}`;
  } else if (st.s === 'closed') subText = '오픈 마감됨';
  // 예약이 없는 상태에도 문구를 준다. 비워 두면 [해제]로 게이트를 푼 순간 —
  //   대기를 건너뛰려는 사람이 가장 확인을 원하는 그때 — 표시가 사라져 뭔가 꺼진 것처럼 보인다.
  else subText = '● 상시 예매 가능 — 오픈 예약 없음';
  if (sub) sub.textContent = subText;
}

// 발동 — 저장된 표식과 대조해 판단한다. 전이(직전 상태)로 판단하면 새로고침이나
// 백그라운드 탭의 타이머 조임으로 그 순간을 건너뛸 때 아무 일도 일어나지 않는다.
export function fireGateEvents() {
  const st = effectiveGate();
  const at = openAtMs();
  if (!at) return;
  const fired = openFiredAt();

  if (st.s === 'open' && fired !== String(at) && !fired.startsWith(`${at}#`)) {
    // 두 탭이 같은 초에 여기 닿을 수 있다. 각자 표식을 쓰고 잠깐 뒤 다시 읽어,
    //   자기 것이 남아 있는 탭만 진행한다. 덮인 탭은 조용히 물러선다.
    claimOpenFire(at);
    setTimeout(() => {
      if (!wonOpenFire(at)) return;              // 다른 탭이 가져갔다
      if (effectiveGate().s !== 'open') return;  // 대기 사이에 상태가 바뀌었다
      markOpenFired(at);
      feedLog('예매가 열렸습니다 — 입장 시작', 'sys');
      toast('예매가 열렸습니다');
      const rush = openRushN();
      const lobbyN = lobbyFakes().length;
      const fresh = isRushFresh();   // 오픈 한참 뒤에 발견한 예약은 투입을 생략한다
      // 이력에는 실제로 진입하는 수를 적는다. 요청값을 적으면 상한에 잘렸을 때
      //   화면 숫자와 이력이 어긋난다(2026-08-31 — "160명 러시"로 기록된 실제 100명 진입).
      const actualRush = Math.min(rush, Math.max(0, MAX_FAKES - activeFakes().length));
      histPush(at, fresh ? actualRush + lobbyN : 0);
      if (fresh && (rush > 0 || lobbyN > 0)) {
        feedLog(`오픈 — 대기실 ${lobbyN}명과 러시 ${rush}명이 동시에 줄을 섭니다`, 'sys');
        // 즉시 투입이 돌고 있으면 잠시 물러섰다 다시 시도한다 — 러시가 조용히 사라지지 않게.
        const tryRush = (left) => {
          if (!addFakes._busy) releaseLobby(rush);
          else if (left > 0) setTimeout(() => tryRush(left - 1), 2000);
        };
        tryRush(5);
      }
    }, 150);
    return;
  }

  if (st.s === 'closed' && fired === String(at)) {
    markOpenFired(`${at}:closed`);
    feedLog('예매가 마감됐습니다 — 오픈 창(10분) 종료', 'sys');
    toast('예매가 마감됐습니다');
    kickWaiting();
  }
}

// ================= 오픈 슬롯·기록 =================
// "지금 +N분"은 누르는 순간을 기준으로 계산해야 한다. 목록을 채운 시각으로 굳혀 두면
//   화면을 열어 두었다가 나중에 적용할 때 그만큼 짧아지고, 해제 후 다시 걸면 옛 시각이
//   그대로 다시 잡혀 "3분을 걸었는데 2분에서 시작"하는 모양이 된다.
//   값에 상대 분(rel)을 담고, 적용 시점에 그 분을 다시 더한다.
const REL_SLOTS = [
  { label: '지금 +1분 (데모)', rel: 1 },
  { label: '지금 +3분 (데모)', rel: 3 },
];

function slotOptions() {
  const out = REL_SLOTS.map(r => ({ label: r.label, ms: Date.now() + r.rel * 60000, rel: r.rel }));
  const d = new Date();
  d.setSeconds(0, 0);
  d.setMinutes(d.getMinutes() < 30 ? 30 : 60);
  // 30분 슬롯은 가까운 둘만 — 방문자가 머무는 시간 안에 걸 수 있는 시각만 의미가 있다.
  //   두 시간 뒤 오픈을 걸고 기다릴 방문자는 없고, 목록만 길어진다.
  for (let i = 0; i < 2; i++) {
    out.push({ label: `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')} 오픈`, ms: d.getTime() });
    d.setMinutes(d.getMinutes() + 30);
  }
  return out;
}

function fillSlots() {
  const sel = $('openSlot'); if (!sel) return;
  const cur = sel.value;
  // 상대 슬롯은 값에 "+N" 을 담는다 — 절대 시각으로 굳히지 않는다.
  sel.innerHTML = slotOptions()
    .map(o => `<option value="${o.rel ? `+${o.rel}` : o.ms}">${o.label}</option>`).join('');
  if ([...sel.options].some(op => op.value === cur)) sel.value = cur;
}

// 선택값을 실제 시각으로 바꾼다. 상대 슬롯이면 지금 기준으로 다시 계산한다.
function slotToMs(v) {
  if (typeof v === 'string' && v.startsWith('+')) return Date.now() + parseInt(v.slice(1)) * 60000;
  return parseInt(v) || 0;
}

function histPush(at, n) {
  const h = [{ at, n }, ...(store.getJSON(K.OPEN_HISTORY, []) || [])].slice(0, 5);
  store.setJSON(K.OPEN_HISTORY, h);
  renderHist();
}
function renderHist() {
  const el = $('openHist'); if (!el) return;
  const h = store.getJSON(K.OPEN_HISTORY, []) || [];
  el.innerHTML = h.map(e => `<li>${fmtAt(e.at)} 오픈${e.n > 0 ? ` · ${e.n}명 러시` : ' · 게이트만'}</li>`).join('');
}

// ================= 콘솔 =================
export function initConsole() {
  fillSlots();
  const sel = $('openSlot');
  const openN = $('openN');
  sel.onmousedown = fillSlots;   // 드롭다운을 여는 순간 데모 슬롯을 현재 시각 기준으로 다시 계산한다

  $('heroN').innerHTML = INJECT_PRESETS.map(v => `<option value="${v}"${v === 30 ? ' selected' : ''}>${v}명</option>`).join('');
  if (openN) {
    // 저장된 러시 인원이 프리셋에 없으면 가장 가까운 값으로 맞춘다
    const cur = openRushN() || 50;
    const snap = RUSH_PRESETS.reduce((a, b) => Math.abs(b - cur) < Math.abs(a - cur) ? b : a);
    openN.innerHTML = RUSH_PRESETS.map(v => `<option value="${v}"${v === snap ? ' selected' : ''}>${v ? `${v}명 러시` : '러시 없음'}</option>`).join('');
  }

  // 첫 방문 기본값 — 1분 뒤 오픈 + 50명 러시. 방문자가 머무는 동안 오픈이 실제로 발동해
  // 대기열이 도는 장면까지 이어진다. 기다리지 않으려면 콘솔의 [해제]로 풀 수 있다.
  if (!openAtMs() && !store.get(K.OPEN_SEEDED)) {
    store.set(K.OPEN_SEEDED, '1');
    store.set(K.OPEN_AT, String(slotOptions()[0].ms));
    store.set(K.OPEN_N, '50');
  }

  $('openSet').onclick = () => {
    const at = slotToMs(sel.value);
    if (at <= Date.now()) { toast('지난 시각입니다 — 슬롯을 다시 선택하세요.', true); fillSlots(); return; }
    const n = Math.max(0, Math.min(MAX_FAKES, parseInt(openN.value) || 0));
    store.set(K.OPEN_AT, String(at));
    store.set(K.OPEN_N, String(n));
    store.del(K.OPEN_FIRED);
    feedLog(`예매 오픈 예약 — ${fmtAt(at)}${n > 0 ? ` · 오픈 시 ${n}명 러시` : ''}`, 'sys');
    renderGate();
  };

  $('openClear').onclick = () => {
    if (!openAtMs()) return;   // 예약이 없으면 할 일이 없다 — 실황에 해제 로그가 도배되지 않게
    // 대기실 인원도 함께 비운다. 예약은 localStorage에 있고 대기실은 이 모듈의 배열에만 있어서,
    //   예약만 지우면 다음 예약이 발동할 때 그 인원이 새 러시에 합류한다("해제했는데 또 나온다").
    //   대기실 관객은 서버에 넣은 적이 없으므로 목록에서 빼면 끝이다.
    const lobbyN = lobbyFakes().length;
    if (lobbyN) S.fakes = S.fakes.filter(f => f.state !== 'lobby');
    clearOpen();
    feedLog(`오픈 예약을 해제했습니다${lobbyN ? ` — 대기실 ${lobbyN}명도 함께 비웠습니다` : ''}`, 'sys');
    renderStrip(); renderGate();
  };

  const body = $('conBody');
  const toggle = $('conToggle');
  const setOpen = (open) => {
    body.classList.toggle('hidden', !open);
    toggle.textContent = open ? '접기' : '펴기';
    toggle.setAttribute('aria-expanded', String(open));
    store.set(K.CONSOLE, open ? '1' : '0');
    // 접혀 있는 동안 실황 폴링이 멈춘다(live.js) — 펴는 즉시 한 번 받아 온다.
    document.dispatchEvent(new CustomEvent('console:toggled', { detail: open }));
  };
  toggle.onclick = () => setOpen(body.classList.contains('hidden'));
  setOpen(store.get(K.CONSOLE) !== '0');

  renderHist();
  document.addEventListener('gate:changed', renderGate);
}

// 새로고침 전 명단을 이어받는다. 서버 진실과는 첫 틱에서 자동으로 맞춰진다 —
// 살아 있으면 그대로 이어지고, 이미 회수됐으면 퇴장으로 정리된다.
export function adoptRoster() {
  const fakes = takeRoster();
  if (!fakes.length) return;
  S.fakes = fakes;
  S.fakes.forEach(f => {
    S.myRids.add(f.rid);
    // 명단에는 bookAt이 없다(저장 안 함). 입장 상태로 이어받은 관객에게 고르는 시간을 새로 준다.
    if (f.state === 'admitted') f.bookAt = Date.now() + bookDelay();
  });
  if (activeFakes().length && !S.simTimer) S.simTimer = setInterval(botTick, TICK_MS);
  feedLog(`이전 화면의 가상 관객 ${S.fakes.length}명을 이어받았습니다`, 'sys');
}
