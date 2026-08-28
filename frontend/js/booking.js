// 서비스 여정 — 신원 발급부터 영화 목록·대기·회차·좌석·결제·완료까지.
// 시뮬레이터(가상 관객)와는 분리돼 있고, 여기서는 화면 앞의 사람 한 명만 다룬다.

import {
  S, K, store, $, show, onScreen, toast, api, uuid, sleep, esc,
  PRICE, sessionSecs, fmtEta, feedLog, effectiveGate, releaseSeats, releaseSlot,
} from './core.js';
import { loadServerConfig, pollBoard, pollEvents } from './live.js';

// ================= 신원 =================
// requestId는 시작할 때 발급하고 로그아웃 때 회수한다. 신원이 없으면 예매에 들어갈 수 없다.
export function renderAuth() {
  const hint = $('idHint');
  if (hint) hint.classList.toggle('hidden', !!S.rid);   // 신원이 없을 때만 시작 안내를 띄운다
  const area = $('authArea');
  if (S.rid) {
    area.innerHTML = `<span class="who"><i>${esc(S.userLabel)}</i> 님</span><button class="btn small ghost" id="logoutBtn">로그아웃</button>`;
    $('logoutBtn').onclick = logout;
  } else {
    area.innerHTML = '<button class="btn small primary" id="loginBtn">익명으로 시작</button>';
    $('loginBtn').onclick = login;
  }
}

export function login() {
  // 인증이 아니라 익명 신원 발급이다 — 신원은 자가발급 UUID(requestId)이고 서버는 형식만 검증한다.
  // 라벨을 UUID 앞자리로 만들어 데모용 익명 신원임이 화면에서 읽히게 한다.
  S.rid = uuid();
  S.userLabel = '관객-' + S.rid.replace(/-/g, '').slice(0, 4);
  S.myRids.add(S.rid);   // 서버 실황에서 내 이벤트는 건너뛴다 — 내 관찰이 이미 더 자세히 그린다
  store.setJSON(K.AUTH, { rid: S.rid, label: S.userLabel });
  renderAuth();
  toast(`익명 신원 발급 — ${S.userLabel}`);
}

// 이탈 공통 절차 — 잡은 좌석 락을 먼저 돌려주고(select와 짝) 자리를 반납한다.
// 진행 흐름 밖(movieId 없음)이면 자연히 아무것도 하지 않는다.
async function releaseAndLeave() {
  if (S.mine.size && S.screeningId && S.rid) await releaseSeats(S.screeningId, [...S.mine], S.rid);
  // 입장 상태였으면 complete, 줄에서 나가는 것이면 leave — 회수 사유를 갈라서 남긴다.
  if (S.rid && S.movieId) await releaseSlot(S.movieId, S.rid, S.active);
}

export async function logout() {
  stopPolling();   // 이탈 절차 중 백그라운드 폴링이 끼어들지 않게 먼저 멈춘다
  await releaseAndLeave();
  resetSession();
  S.rid = null; S.userLabel = null;
  store.del(K.AUTH);
  renderAuth(); loadMovies();
  toast('로그아웃되었습니다');
}

// ================= 1) 영화 목록 =================
export async function loadMovies() {
  stopActiveTimer();
  show('movies');
  const { ok, data } = await api('/api/movies');
  const list = $('movieList');
  list.innerHTML = '';
  // 요청이 실패한 것과 목록이 비어 있는 것을 나눈다 — 후자는 정상 응답이라 재시도할 일이 아니다.
  if (!ok) {
    // 순단(롤링 재시작 등)에 걸린 방문자가 새로고침 없이 살아나게 자동으로 다시 시도한다.
    list.innerHTML = '<p class="load-err">상영 정보를 불러오지 못했습니다 — 5초 후 다시 시도합니다.</p>';
    clearTimeout(loadMovies._t);
    loadMovies._t = setTimeout(loadMovies, 5000);
    return;
  }
  if (!data || !data.length) {
    list.innerHTML = '<p class="load-empty">지금 예매할 수 있는 상영이 없습니다.</p>';
    return;
  }
  S.simMovieId = data[0].id;     // 시뮬레이터가 쓸 영화
  loadServerConfig();            // 세션 수명 — 배포 때만 바뀌므로 한 번만 읽는다
  pollBoard(); pollEvents();     // 3초 주기를 기다리지 않고 즉시 — 첫 화면부터 현황판·실황이 찬다

  data.forEach(m => {
    const time = m.broadcastAt ? String(m.broadcastAt).replace('T', ' ').slice(0, 16) : '';
    const card = document.createElement('div');
    card.className = 'event-banner';
    card.innerHTML =
      '<div class="eb-poster"><span class="poster-live">● LIVE</span>' +
        '<div class="eb-art"><b>KBO</b><i>ALL-STAR</i><em>2 0 2 6</em></div></div>' +
      '<div class="eb-info"><span class="badge">단독 생중계</span>' +
        `<h3>${esc(m.title)}</h3><div class="time">${esc(time)} 방송</div>` +
        '<div class="eb-actions"><button class="btn primary big go">예매하기 →</button></div></div>';

    card.querySelector('.go').onclick = () => {
      if (!S.rid) { toast('우측 상단 [익명으로 시작]을 눌러 신원을 만든 뒤 이용해주세요.', true); return; }
      enter(m.id, m.title);
    };
    list.appendChild(card);
  });
  document.dispatchEvent(new CustomEvent('gate:changed'));   // 새로 그린 카드에 게이트 상태를 입힌다
}

// ================= 2) 입장 요청 =================
export async function enter(movieId, title) {
  if (enter._busy) return;   // 연타 가드 — 같은 requestId로 enter가 중복 발사되지 않게
  const gs = effectiveGate().s;
  if (gs === 'before' || gs === 'closed') {
    store.del(K.FLOW);   // 게이트에 막힌 진행 흐름은 죽은 것이라 지운다(리로드마다 오류가 반복되지 않게)
    toast(gs === 'before' ? '아직 오픈 전입니다.' : '예매가 마감됐습니다.', true);
    return;
  }
  enter._busy = true;
  try {
    S.movieId = movieId; S.movieTitle = title;
    // 새로고침 복구용 — 진행 중임을 저장한다. 시작 시 이 값으로 enter를 다시 부른다.
    // 서버는 같은 requestId를 보고 active면 유지, waiting이면 꼬리로 민다.
    store.setJSON(K.FLOW, { movieId, title });

    const { ok, status, data } = await api('/api/admission/enter', { method: 'POST', body: { movieId, requestId: S.rid } });
    // 서버의 정직한 실패(Kafka 발행이 끝나지 못한 경우): 오류로 깨지지 말고 지연으로 안내한다.
    if (status === 503 && data && data.status === 'RETRY_LATER') {
      toast('입장 처리가 지연되고 있어요. 잠시 후 다시 시도해주세요.', true);
      return;
    }
    if ((!ok && status !== 202) || !data) { toast('입장 요청 실패', true); return; }

    if (data.status === 'WAITING') {          // 202 → 대기
      $('waitMovieTitle').textContent = title || '대기 중';
      $('waitRank').textContent = data.rank;
      $('waitBehind').textContent = Math.max(0, (data.totalWaiting || 0) - (data.rank || 0));
      $('waitEta').textContent = '계산 중';
      feedLog(`나(${S.userLabel}) 대기열 합류 — ${data.rank}번째`, 'me');
      show('waiting'); startPolling();
    } else {                                   // 200 → 입장
      if (data.status === 'ADMITTED') feedLog(`나(${S.userLabel}) 즉시 입장 — 정원 여유`, 'me');
      goActive();
    }
  } finally { enter._busy = false; }
}

// ================= 3) 대기 폴링 =================
// 서버 push가 없다. 클라이언트가 주기적으로 자기 순번과 입장 여부를 가져온다.
// 응답은 세 상태다: WAITING(순번·뒤 인원·예상) / ADMITTED(입장) / EXPIRED(타임아웃·이탈).
// 입장 뒤에도 느린 폴링을 유지해 서버의 세션 만료를 감지한다.
function startPolling() { stopPolling(); pollPosition(); }
export function stopPolling() { if (S.pollTimer) { clearTimeout(S.pollTimer); S.pollTimer = null; } }
function scheduleNext(ms) { stopPolling(); S.pollTimer = setTimeout(pollPosition, ms + Math.random() * 300); }   // jitter로 동시 폴링을 흩는다

async function pollPosition() {
  if (!S.rid || !S.movieId) return;
  const epoch = S.pollEpoch;
  const { ok, data } = await api(`/api/admission/position?movieId=${encodeURIComponent(S.movieId)}&requestId=${encodeURIComponent(S.rid)}`);
  if (epoch !== S.pollEpoch) return;   // 이탈·리셋 뒤 도착한 응답은 버린다 — 떠난 화면이 되살아나지 않게
  if (!ok || !data) { scheduleNext(1000); return; }
  S.lastPollAt = Date.now();

  if (data.status === 'ADMITTED') {
    if (!S.active) goActive();        // 대기에서 입장으로 전환(최초 1회)
    else scheduleNext(5000);          // 이미 입장 상태면 만료 감시용 느린 폴링
    return;
  }
  if (data.status === 'EXPIRED') { serverKicked(); return; }

  animateRank(data.position);
  $('waitBehind').textContent = data.behind ?? 0;            // 0도 유효값(꼴찌 = 뒤에 0명)
  $('waitEta').textContent = fmtEta(data.etaSeconds ?? -1);  // 0("곧 입장")도 유효값이라 누락일 때만 -1
  const p = data.position || 0;
  scheduleNext(p > 100 ? 5000 : p > 20 ? 2000 : 1000);       // 뒤쪽일수록 느슨하게
}

function animateRank(target) {
  const el = $('waitRank');
  const cur = parseInt(el.textContent) || target;
  clearInterval(animateRank._t);
  if (cur === target) { el.textContent = target; return; }
  // 격차가 크면 즉시 표시한다 — 한 칸씩 세는 연출이 실제 순번을 몇 초씩 다르게 보이게 하지 않도록.
  if (Math.abs(cur - target) > 30) { el.textContent = target; return; }
  const step = cur > target ? -1 : 1;
  let v = cur;
  animateRank._t = setInterval(() => {
    v += step; el.textContent = v;
    if (v === target) clearInterval(animateRank._t);
  }, 40);
}

function goActive() {
  S.active = true;
  startActiveTimer();
  toScreenings();
  scheduleNext(5000);   // 입장 동안에도 폴링을 유지해 세션 만료를 감지한다
}

function serverKicked() {
  toast(S.active
    ? '입장 시간이 초과되어 자리가 반환되었습니다. 다시 입장할 수 있습니다.'
    : '자리 비움으로 대기열에서 나갔습니다 — 다시 줄을 설 수 있습니다.', true);
  resetSession(); loadMovies();
}

// 백그라운드 탭에서 돌아오면 즉시 한 번 폴링한다 — 브라우저 타이머 조임으로 밀린 상태를 바로 맞춘다.
document.addEventListener('visibilitychange', () => {
  if (!document.hidden && S.rid && S.movieId) pollPosition();
});

// ================= 세션 카운트다운 =================
function startActiveTimer() {
  // 새로고침 복구면 저장된 만료 시각을 이어받는다 — 초기화된 60초를 보여주다 서버 회수를 맞지 않게.
  S.activeUntil = (S.resumeUntil && S.resumeUntil > Date.now()) ? S.resumeUntil : Date.now() + sessionSecs() * 1000;
  S.resumeUntil = 0;
  const flow = store.getJSON(K.FLOW);
  if (flow) { flow.activeUntil = S.activeUntil; store.setJSON(K.FLOW, flow); }

  clearInterval(S.timerTick);
  const render = () => {
    const left = Math.max(0, Math.round((S.activeUntil - Date.now()) / 1000));
    const p = (n) => String(n).padStart(2, '0');
    $('activeTimer').querySelector('b').textContent = `${p(Math.floor(left / 60))}:${p(left % 60)}`;
    $('activeTimer').classList.toggle('warn', left <= 10);
    // 0이 지나도 서버 확정이 오지 않으면 안전망으로 로컬에서 내보낸다.
    // 여유 25초 = 서버 감지의 최대 지연(타임아웃 루프 10초 + 느린 폴링 약 5초)보다 넉넉하게 — 서버가 진실이다.
    if (Date.now() - S.activeUntil > 25000) serverKicked();
  };
  render();
  S.timerTick = setInterval(render, 1000);
}
function stopActiveTimer() {
  clearInterval(S.timerTick);
  S.activeUntil = 0;
  $('activeTimer').classList.add('hidden');
}

// ================= 4) 회차 선택 =================
// 입장 직후에는 booking이 아직 입장 이벤트를 못 받아 403이 날 수 있다. 그 틈을 재시도로 흡수한다.
async function toScreenings() {
  show('entering');
  for (let i = 0; i < 12; i++) {
    const { ok, status, data } = await api(`/api/screenings?movieId=${encodeURIComponent(S.movieId)}&requestId=${encodeURIComponent(S.rid)}`);
    if (ok) { renderScreenings(data || []); return; }
    if (status === 403) { await sleep(300); continue; }
    toast('회차 조회 실패', true); return;
  }
  toast('입장 처리가 지연됩니다.', true);
  loadMovies();
}

function renderScreenings(list) {
  $('scMovieTitle').textContent = (S.movieTitle || '') + ' · 회차 선택';
  const box = $('screeningList');
  box.innerHTML = '';
  const byBranch = {};
  list.forEach(s => (byBranch[s.branch] = byBranch[s.branch] || []).push(s));

  Object.keys(byBranch).forEach(branch => {
    const wrap = document.createElement('div');
    wrap.className = 'branch';
    wrap.innerHTML = `<p class="branch-name">${esc(branch)}</p>`;
    const cards = document.createElement('div');
    cards.className = 'screen-cards';

    byBranch[branch].sort((a, b) => a.screenNo - b.screenNo).forEach(s => {
      const soldout = s.remain <= 0;
      const card = document.createElement('button');
      card.type = 'button';
      card.className = 'sc-card' + (soldout ? ' soldout' : '');
      card.disabled = soldout;
      card.innerHTML = `<span class="no">${esc(s.screenNo)}관</span>` + (soldout
        ? '<span class="remain">매진</span>'
        : `<span class="remain${s.remain <= 20 ? ' low' : ''}">${esc(s.remain)}/${esc(s.total)}석</span>`);
      if (!soldout) card.onclick = () => openSeats(s.screeningId, `${branch} ${s.screenNo}관`);
      cards.appendChild(card);
    });
    wrap.appendChild(cards);
    box.appendChild(wrap);
  });
  show('screenings');
}

// ================= 5) 좌석 =================
export async function openSeats(screeningId, label, keepMine = false) {
  // keepMine: 결제에서 뒤로 온 것처럼 내 락이 살아 있는 재진입. S.mine을 비우면 서버 응답(taken)에
  // 소유자 구분이 없어 내가 잡은 좌석이 남의 것으로 보인다.
  S.screeningId = screeningId;
  S.screeningLabel = label;
  if (!keepMine) S.mine = new Set();
  $('seatScreeningTitle').textContent = label;
  await loadSeats();
  show('seats');
  startSeatPoll();
}

async function loadSeats() {
  const { ok, data } = await api(`/api/seats?screeningId=${encodeURIComponent(S.screeningId)}&requestId=${encodeURIComponent(S.rid)}`);
  const grid = $('seatGrid');
  // 조회가 실패하면 좌석도를 비운 채 두지 않는다 — 빈 그리드는 매진이나 고장으로 보이고,
  // 토스트는 몇 초 뒤 사라져 남는 설명이 없다. 화면 안에 상태와 재시도를 함께 남긴다.
  if (!ok) {
    toast('좌석 정보를 불러오지 못했습니다.', true);
    grid.style.removeProperty('--seat-cols');
    grid.innerHTML = '<p class="load-err">좌석 정보를 불러오지 못했습니다.'
      + ' <button type="button" class="linklike" id="seatRetry">다시 시도</button></p>';
    const retry = $('seatRetry');
    if (retry) retry.onclick = () => loadSeats();
    return;
  }
  grid.innerHTML = '';

  const rows = {};
  let maxCol = 0;
  (data || []).forEach(s => {
    (rows[s.row] = rows[s.row] || []).push(s);
    if (s.col > maxCol) maxCol = s.col;
  });
  // 열 수는 서버가 정한다(회차당 좌석 구성). 화면에 고정값을 두면 구성이 바뀔 때 좌석도가 어긋난다.
  grid.style.setProperty('--seat-cols', maxCol + 1);

  Object.keys(rows).map(Number).sort((a, b) => a - b).forEach(r => {
    const label = document.createElement('div');
    label.className = 'row-label';
    label.textContent = String.fromCharCode(65 + r);
    grid.appendChild(label);

    rows[r].sort((a, b) => a.col - b.col).forEach(s => {
      const mine = S.mine.has(s.seatNo);
      const cell = document.createElement('button');
      cell.type = 'button';
      cell.className = 'seat' + (mine ? ' mine' : s.taken ? ' taken' : '');
      cell.dataset.seat = s.seatNo;
      cell.title = s.seatNo;
      cell.setAttribute('aria-label', `${s.seatNo} ${mine ? '내 선택' : s.taken ? '선점됨' : '선택 가능'}`);
      if (s.taken && !mine) cell.disabled = true;
      else cell.onclick = () => toggleSeat(s.seatNo, cell);
      grid.appendChild(cell);
    });
  });
  updateSelBar();
}

async function toggleSeat(seatNo, cell) {
  if (toggleSeat._busy) return;   // 연타 가드 — 두 번째 요청이 409를 받아 내 좌석이 남의 것처럼 보이는 것 방지
  toggleSeat._busy = true;
  try {
    if (S.mine.has(seatNo)) {
      await releaseSeats(S.screeningId, [seatNo], S.rid);
      S.mine.delete(seatNo);
      cell.classList.remove('mine');
      updateSelBar();
      return;
    }
    const { ok, status } = await api('/api/seats/select', { method: 'POST', body: { screeningId: S.screeningId, seatNos: [seatNo], requestId: S.rid } });
    if (ok) { S.mine.add(seatNo); cell.classList.add('mine'); updateSelBar(); }
    else if (status === 409) { cell.classList.add('taken'); cell.disabled = true; toast('이미 선점된 좌석입니다.', true); }
    else if (status === 403) { serverKicked(); }
    else toast('점유 실패', true);
  } finally { toggleSeat._busy = false; }
}

function updateSelBar() {
  const arr = [...S.mine].sort();
  $('mySeats').textContent = arr.length ? arr.join(', ') : '없음';
  $('myPrice').textContent = (arr.length * PRICE).toLocaleString();
  $('toPay').disabled = arr.length === 0;
}

function startSeatPoll() {
  stopSeatPoll();
  // 좌석을 누르는 중에는 다시 그리기를 쉰다 — 응답 직전 그리드가 교체되면 방금 잡은 좌석이 회색으로 보인다.
  S.seatPollTimer = setInterval(() => {
    if (onScreen('seats') && !toggleSeat._busy) loadSeats();
  }, 5000);
}
function stopSeatPoll() { if (S.seatPollTimer) { clearInterval(S.seatPollTimer); S.seatPollTimer = null; } }

// ================= 6) 결제·완료 =================
export function toPay() {
  const arr = [...S.mine].sort();
  if (!arr.length) return;
  stopSeatPoll();
  $('payScreening').textContent = S.screeningLabel;
  $('paySeats').textContent = arr.join(', ');
  $('payAmount').textContent = (arr.length * PRICE).toLocaleString();
  $('payStatus').classList.add('hidden');
  show('pay');
}

export async function confirmPay() {
  const arr = [...S.mine].sort();
  $('payStatus').classList.remove('hidden');
  $('payConfirm').disabled = true;
  const idem = `${S.rid}:${S.screeningId}:${arr.join('-')}`;
  const { ok, status, data } = await api('/api/bookings', { method: 'POST', body: { screeningId: S.screeningId, seatNos: arr, requestId: S.rid, idempotencyKey: idem } });
  $('payConfirm').disabled = false;
  $('payStatus').classList.add('hidden');

  if (ok) { showDone(data); return; }
  if (status === 409) {
    // 일부 좌석만 만료·탈취된 경우, 아직 유효한 내 락이 고아로 남지 않게 먼저 돌려준다.
    toast('좌석이 만료되었거나 이미 판매되었습니다.', true);
    await releaseSeats(S.screeningId, arr, S.rid);
    openSeats(S.screeningId, S.screeningLabel);
    return;
  }
  if (status === 403) { serverKicked(); return; }
  toast('결제 실패', true);
}

function showDone(b) {
  feedLog(`나(${S.userLabel}) 예매 완료 — ${b.bookingId}`, 'me');
  $('doneNo').textContent = b.bookingId;
  $('doneScreening').textContent = S.screeningLabel;
  $('doneSeats').textContent = (b.seatNos || []).sort().join(', ');
  $('doneAmount').textContent = (b.price || 0).toLocaleString();
  // 세션 상태를 정리한다 — 남겨두면 탭에 돌아왔을 때 폴링이 EXPIRED(예매로 큐에서 빠짐)를 받아
  // 완료 화면에서 시간 초과 회수로 오인한다. 화면 값은 이미 그려져 있어 지워도 안전하다.
  resetSession();
  show('done');
}

// ================= 네비게이션 =================
export async function backFromSeats() {
  const arr = [...S.mine];
  if (arr.length) await releaseSeats(S.screeningId, arr, S.rid);
  S.mine = new Set();
  stopSeatPoll();
  toScreenings();
}

export async function goHome() {
  stopPolling();             // 이탈 절차 중 폴링이 끼어들지 않게 먼저 멈춘다
  await releaseAndLeave();   // 홈으로 가는 것은 예매 포기다
  resetSession();
  loadMovies();
}

// 세션 상태를 전부 비운다. epoch를 올려 이미 날아간 폴링 응답도 버린다.
export function resetSession() {
  S.pollEpoch++;
  stopPolling(); stopSeatPoll(); stopActiveTimer();
  S.active = false;
  S.mine = new Set();
  S.screeningId = null; S.screeningLabel = null;
  S.movieId = null; S.movieTitle = null;
  store.del(K.FLOW);
}
