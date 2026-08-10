/* CGV 예매 프론트 — 대기열 입장·좌석 선택·예매 확정 흐름.
   추가: 로그인(requestId 발급/회수)·시연용 대기열 생성기·active 1분 카운트다운(+TIMEOUT 수신). */

const PRICE = 6000;
const SESSION_SEC = 60;   // 서버 SESSION_TIMEOUT과 정합(데모 1분). 카운트다운 표시용.

// ================= 상태 =================
const S = {
  rid: null, userLabel: null,                 // 로그인 세션(requestId + 표시 라벨)
  movieId: null, movieTitle: null,
  pollTimer: null, active: false,             // 폴링 타이머 + 입장(active) 여부
  screeningId: null, screeningLabel: null,
  seatMeta: {}, mine: new Set(), seatPollTimer: null,
  activeUntil: 0, timerTick: null,            // active 카운트다운
  simMovieId: null, simMovieTitle: null, fakes: [], simTimer: null,// 시뮬레이터
  lastPollAt: 0,                              // 마지막 폴링 시각(생존 신호 표시)
  pollEpoch: 0, simGen: 0, resumeUntil: 0,    // 폴링 세대(이탈 후 유령 응답 폐기)·시뮬 세대(내보내기 취소)·세션 복원
};

// ================= DOM/화면 =================
const $ = (id) => document.getElementById(id);
const screens = ['movies','waiting','entering','screenings','seats','pay','done'];
const ACTIVE_SCREENS = ['entering','screenings','seats','pay'];   // 카운트다운 보일 화면
function show(name) {
  screens.forEach(s => $('screen-' + s).classList.toggle('hidden', s !== name));
  $('activeTimer').classList.toggle('hidden', !(ACTIVE_SCREENS.includes(name) && S.activeUntil));
}
function toast(msg, err) {
  const t = $('toast'); t.textContent = msg; t.className = 'toast' + (err ? ' err' : '');
  clearTimeout(toast._t); toast._t = setTimeout(() => t.classList.add('hidden'), 2400);
}

// ================= API =================
async function api(path, opts = {}) {
  // fetch 자체 예외(커넥션 거부·순단 — 백엔드 재시작 중 흔함)도 잡는다.
  // 안 잡으면 pollPosition이 throw로 죽어 폴링이 영구 정지(재예약 없이) → 화면이 조용히 멈춤.
  try {
    const res = await fetch(path, {
      method: opts.method || 'GET',
      headers: opts.body ? { 'Content-Type': 'application/json' } : undefined,
      body: opts.body ? JSON.stringify(opts.body) : undefined,
    });
    let data = null; try { data = await res.json(); } catch {}
    return { ok: res.ok, status: res.status, data };
  } catch {
    return { ok: false, status: 0, data: null };   // status 0 = 네트워크 실패. 호출부가 재시도 판단.
  }
}
const uuid = () => (crypto.randomUUID ? crypto.randomUUID() : 'u' + Date.now() + Math.random().toString(16).slice(2));
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const esc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;' }[c]));

// ================= 로그인/로그아웃 =================
// requestId는 로그인 시 발급, 로그아웃 시 회수. 비로그인이면 예매 불가.
function renderAuth() {
  const hint = $('idHint'); if (hint) hint.classList.toggle('hidden', !!S.rid);   // 신원 없을 때만 시작 안내
  const a = $('authArea');
  if (S.rid) {
    a.innerHTML = `<span class="who"><i>${esc(S.userLabel)}</i> 님</span><button class="btn small ghost" id="logoutBtn">로그아웃</button>`;
    $('logoutBtn').onclick = logout;
  } else {
    a.innerHTML = `<button class="btn small primary" id="loginBtn">익명으로 시작</button>`;
    $('loginBtn').onclick = login;
  }
}
function login() {
  // 인증이 아니라 익명 신원 발급이다 — 신원 = 자가발급 UUID(requestId), 서버는 검증만 한다.
  // 라벨을 UUID 앞자리로 만들어 "데모용 익명 신원"임이 화면에서 읽히게 한다.
  S.rid = uuid();
  S.userLabel = '관객-' + S.rid.replace(/-/g, '').slice(0, 4);
  localStorage.setItem('cgv_auth', JSON.stringify({ rid: S.rid, label: S.userLabel }));
  renderAuth(); toast(`익명 신원 발급 — ${S.userLabel}`);
}
// 이탈 공통 절차 — 잡은 좌석 락 먼저 반환(select↔release 짝, 안 하면 TTL 45초 유령 점유) 후 자리 반납.
// 진행 흐름 밖(movieId 없음)이면 자연히 아무것도 안 한다.
async function releaseAndLeave() {
  if (S.mine.size && S.screeningId && S.rid) {
    await api('/api/seats/release', { method: 'POST', body: { screeningId: S.screeningId, seatNos: [...S.mine], requestId: S.rid } });
  }
  if (S.rid && S.movieId) { await api('/api/admission/leave', { method: 'POST', body: { movieId: S.movieId, requestId: S.rid } }); }
}
async function logout() {
  stopPolling();   // 이탈 절차 중 백그라운드 폴링이 끼어들지 않게 먼저 정지
  await releaseAndLeave();
  resetSession();
  S.rid = null; S.userLabel = null;
  localStorage.removeItem('cgv_auth');
  renderAuth(); loadMovies(); toast('로그아웃되었습니다');
}

// ================= 1) 영화 목록 =================
async function loadMovies() {
  stopActiveTimer();
  show('movies');
  const { ok, data } = await api('/api/movies');
  const list = $('movieList'); list.innerHTML = '';
  if (!ok || !data || !data.length) {
    // 순단(롤링 재시작 등)에 걸린 방문자가 새로고침 없이 살아나게 자동 재시도.
    list.innerHTML = '<p class="load-err">상영 정보를 불러오지 못했습니다 — 5초 후 다시 시도합니다.</p>';
    clearTimeout(loadMovies._t); loadMovies._t = setTimeout(loadMovies, 5000);
    return;
  }
  S.simMovieId = data[0].id; S.simMovieTitle = data[0].title;   // 시뮬레이터·히어로가 쓸 영화
  data.forEach(m => {
    const time = m.broadcastAt ? m.broadcastAt.replace('T', ' ').slice(0, 16) : '';
    const card = document.createElement('div');
    card.className = 'event-banner';
    card.innerHTML =
      `<div class="eb-poster"><span class="poster-live">● LIVE</span>` +
      `<div class="eb-art"><b>KBO</b><i>ALL-STAR</i><em>2 0 2 6</em></div></div>` +
      `<div class="eb-info"><span class="badge">단독 생중계</span><h3>${esc(m.title)}</h3>` +
      `<div class="time">${time} 방송</div>` +
      `<button class="btn primary big go">예매하기 →</button></div>`;
    card.querySelector('.go').onclick = () => {   // 신원 없이는 예매 진입 불가
      if (!S.rid) { toast('우측 상단 [익명으로 시작]을 눌러 신원을 만든 뒤 이용해주세요.', true); return; }
      enter(m.id, m.title);
    };
    list.appendChild(card);
  });
}

// ================= enter =================
async function enter(movieId, title) {
  if (enter._busy) return;   // 연타 가드 — 같은 requestId로 enter 중복 POST 방지
  const gs = gateState().s;   // 프론트 게이트(연출) — 오픈 전·마감 후 차단
  if (gs === 'before' || gs === 'closed') {
    localStorage.removeItem('cgv_flow');   // 게이트에 막힌 진행 흐름은 죽은 것 — 리로드마다 오류 토스트 반복 방지
    toast(gs === 'before' ? '아직 오픈 전입니다.' : '예매가 마감됐습니다.', true);
    return;
  }
  enter._busy = true;
  try {
    S.movieId = movieId; S.movieTitle = title;
    // 새로고침 복구용 — 대기/입장 진행 중임을 저장. init()이 F5 후 이걸로 enter 재호출.
    // → 서버가 같은 requestId를 보고 active=유지 / waiting=꼬리로 밀기.
    localStorage.setItem('cgv_flow', JSON.stringify({ movieId, title }));
    const { ok, status, data } = await api('/api/admission/enter', { method: 'POST', body: { movieId, requestId: S.rid } });
    // 서버의 정직한 실패(Kafka 발행 실패 → 보상 롤백, queue): 에러로 깨지지 말고 지연 안내.
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
      show('waiting'); startPolling();         // SSE 대신 GET /position 폴링
    } else {                                   // 200 ADMITTED → active
      if (data.status === 'ADMITTED') feedLog(`나(${S.userLabel}) 즉시 입장 — 정원 여유`, 'me');
      goActive();
    }
  } finally { enter._busy = false; }
}

// ================= 2) 대기 폴링 (GET /position) =================
// 서버 push(SSE) 없음. 클라가 주기적으로 내 순번·입장여부를 pull.
// 응답 3-state: WAITING(순번·뒤인원·예상) / ADMITTED(입장) / EXPIRED(타임아웃·이탈).
// active 진입 후에도 느린 폴링 유지 → 서버 세션 타임아웃(EXPIRED) 감지(SSE TIMEOUT 대체).
function startPolling() { stopPolling(); pollPosition(); }
function stopPolling() { if (S.pollTimer) { clearTimeout(S.pollTimer); S.pollTimer = null; } }
function scheduleNext(ms) { stopPolling(); S.pollTimer = setTimeout(pollPosition, ms + Math.random() * 300); } // jitter

async function pollPosition() {
  if (!S.rid || !S.movieId) return;
  const epoch = S.pollEpoch;
  const { ok, data } = await api(`/api/admission/position?movieId=${encodeURIComponent(S.movieId)}&requestId=${encodeURIComponent(S.rid)}`);
  if (epoch !== S.pollEpoch) return;   // 이탈·리셋 뒤 도착한 응답 폐기 — 떠난 화면이 부활하지 않게
  if (!ok || !data) { scheduleNext(1000); return; }   // 일시 오류 → 재시도
  S.lastPollAt = Date.now();   // 생존 신호 도장이 찍힌 시각(대기 화면 표시용)

  if (data.status === 'ADMITTED') {
    if (!S.active) goActive();        // 대기→입장 전환(최초 1회)
    else scheduleNext(5000);          // 이미 active면 EXPIRED 감시용 느린 폴링
    return;
  }
  if (data.status === 'EXPIRED') { serverKicked(); return; }   // 큐에서 빠짐(타임아웃/이탈/세션만료)

  // WAITING — 순번·뒤인원·예상시간 갱신 + 적응형 주기
  animateRank(data.position);
  $('waitBehind').textContent = data.behind ?? 0;         // 0도 유효값(꼴찌=뒤에 0명). omitempty 대비 ?? 0
  $('waitEta').textContent = fmtEta(data.etaSeconds ?? -1); // eta=0("곧 입장")도 유효값 — 누락 시에만 -1(계산 중)
  const p = data.position || 0;
  scheduleNext(p > 100 ? 5000 : p > 20 ? 2000 : 1000);
}
function fmtEta(sec) {
  if (sec == null || sec < 0) return '계산 중';
  if (sec < 60) return `${sec}초`;
  return `${Math.floor(sec / 60)}분 ${sec % 60}초`;
}
function animateRank(target) {
  const el = $('waitRank'); const cur = parseInt(el.textContent) || target;
  clearInterval(animateRank._t);
  if (cur === target) { el.textContent = target; return; }
  // 격차가 크면 즉시 표시 — 1씩 세는 연출이 실제 순번을 몇 초씩 속이지 않게
  if (Math.abs(cur - target) > 30) { el.textContent = target; return; }
  const step = cur > target ? -1 : 1; let v = cur;
  animateRank._t = setInterval(() => { v += step; el.textContent = v; if (v === target) clearInterval(animateRank._t); }, 40);
}

// active 진입: 카운트다운 시작 + 회차선택 + EXPIRED 감시 느린 폴링 유지
function goActive() {
  S.active = true;
  startActiveTimer();
  toScreenings();
  scheduleNext(5000);         // active 동안 /position 느린 폴링 → 세션 만료(EXPIRED) 감지
}
function serverKicked() {      // EXPIRED = 진짜 퇴장. 대기 중이었는지 입장 중이었는지 문구를 가른다.
  toast(S.active
    ? '입장 시간이 초과되어 자리가 반환되었습니다. 다시 입장할 수 있습니다.'
    : '자리 비움으로 대기열에서 나갔습니다 — 다시 줄을 설 수 있습니다.', true);
  resetSession(); loadMovies();
}
// 백그라운드 탭에서 돌아오면 즉시 폴링 1회 — 브라우저 타이머 스로틀로 밀린 상태를 바로 확정.
document.addEventListener('visibilitychange', () => { if (!document.hidden && S.rid && S.movieId) pollPosition(); });

// ================= active 1분 카운트다운 (입장자만) =================
function startActiveTimer() {
  // 새로고침 복구면 저장된 만료 시각을 이어받는다 — 리셋된 60초를 보여주다 서버 킥을 맞지 않게.
  S.activeUntil = (S.resumeUntil && S.resumeUntil > Date.now()) ? S.resumeUntil : Date.now() + SESSION_SEC * 1000;
  S.resumeUntil = 0;
  try {
    const flow = JSON.parse(localStorage.getItem('cgv_flow') || 'null');
    if (flow) { flow.activeUntil = S.activeUntil; localStorage.setItem('cgv_flow', JSON.stringify(flow)); }
  } catch {}
  clearInterval(S.timerTick);
  const render = () => {
    const left = Math.max(0, Math.round((S.activeUntil - Date.now()) / 1000));
    const b = $('activeTimer').querySelector('b');
    b.textContent = `${String(Math.floor(left / 60)).padStart(2, '0')}:${String(left % 60).padStart(2, '0')}`;
    $('activeTimer').classList.toggle('warn', left <= 10);
    // 0 이후 서버 확정(EXPIRED)이 안 오면 안전망으로 로컬 퇴장.
    // 마진 25s = 서버 감지 최대지연(타임아웃 루프 10s + 느린 폴링 약 5.3s)보다 넉넉히 — 서버보다 먼저 킥하지 않게(서버가 진실).
    if (Date.now() - S.activeUntil > 25000) serverKicked();
  };
  render(); S.timerTick = setInterval(render, 1000);
}
function stopActiveTimer() { clearInterval(S.timerTick); S.activeUntil = 0; $('activeTimer').classList.add('hidden'); }

// ================= 3) 회차 선택 (403 재시도) =================
async function toScreenings() {
  show('entering');
  for (let i = 0; i < 12; i++) {
    const { ok, status, data } = await api(`/api/screenings?movieId=${encodeURIComponent(S.movieId)}&requestId=${encodeURIComponent(S.rid)}`);
    if (ok) { renderScreenings(data || []); return; }   // 빈 바디 방어
    if (status === 403) { await sleep(300); continue; }
    toast('회차 조회 실패', true); return;
  }
  toast('입장 처리가 지연됩니다.', true); loadMovies();
}
function renderScreenings(list) {
  $('scMovieTitle').textContent = (S.movieTitle || '') + ' · 회차 선택';
  const box = $('screeningList'); box.innerHTML = '';
  const byBranch = {};
  list.forEach(s => (byBranch[s.branch] = byBranch[s.branch] || []).push(s));
  Object.keys(byBranch).forEach(branch => {
    const wrap = document.createElement('div'); wrap.className = 'branch';
    wrap.innerHTML = `<p class="branch-name">${esc(branch)}</p>`;
    const cards = document.createElement('div'); cards.className = 'screen-cards';
    byBranch[branch].sort((a, b) => a.screenNo - b.screenNo).forEach(s => {
      const soldout = s.remain <= 0;
      const c = document.createElement('div');
      c.className = 'sc-card' + (soldout ? ' soldout' : '');
      c.innerHTML = `<div class="no">${s.screenNo}관</div>` +
        (soldout ? `<div class="remain">매진</div>` : `<div class="remain${s.remain <= 20 ? ' low' : ''}">${s.remain}/${s.total}석</div>`);
      if (!soldout) c.onclick = () => openSeats(s.screeningId, `${branch} ${s.screenNo}관`);
      cards.appendChild(c);
    });
    wrap.appendChild(cards); box.appendChild(wrap);
  });
  show('screenings');
}

// ================= 4) 좌석도 =================
async function openSeats(screeningId, label, keepMine = false) {
  // keepMine: 결제→뒤로가기처럼 내 락이 살아있는 재진입 — S.mine을 비우면
  // 서버 응답(taken)에 소유자 구분이 없어 내 좌석이 회색으로 죽어버림.
  S.screeningId = screeningId; S.screeningLabel = label;
  if (!keepMine) S.mine = new Set();
  $('seatScreeningTitle').textContent = label;
  await loadSeats(); show('seats'); startSeatPoll();
}
async function loadSeats() {
  const { ok, data } = await api(`/api/seats?screeningId=${encodeURIComponent(S.screeningId)}&requestId=${encodeURIComponent(S.rid)}`);
  if (!ok) { toast('좌석 정보를 불러오지 못했습니다.', true); return; }
  S.seatMeta = {};
  const grid = $('seatGrid'); grid.innerHTML = '';
  const rows = {};
  (data || []).forEach(s => { S.seatMeta[s.seatNo] = { row: s.row, col: s.col }; (rows[s.row] = rows[s.row] || []).push(s); });   // 빈 바디 방어
  Object.keys(rows).map(Number).sort((a, b) => a - b).forEach(r => {
    const label = document.createElement('div'); label.className = 'row-label';
    label.textContent = String.fromCharCode(65 + r); grid.appendChild(label);
    rows[r].sort((a, b) => a.col - b.col).forEach(s => {
      const mine = S.mine.has(s.seatNo);
      const cell = document.createElement('div');
      cell.className = 'seat' + (mine ? ' mine' : s.taken ? ' taken' : '');
      cell.dataset.seat = s.seatNo; cell.title = s.seatNo;
      if (!s.taken || mine) cell.onclick = () => toggleSeat(s.seatNo, cell);
      grid.appendChild(cell);
    });
  });
  updateSelBar();
}
async function toggleSeat(seatNo, cell) {
  if (toggleSeat._busy) return;   // 연타 가드 — 내 두 번째 요청이 409를 받아 내 좌석이 회색으로 죽는 것 방지
  toggleSeat._busy = true;
  try {
    if (S.mine.has(seatNo)) {
      await api('/api/seats/release', { method: 'POST', body: { screeningId: S.screeningId, seatNos: [seatNo], requestId: S.rid } });
      S.mine.delete(seatNo); cell.classList.remove('mine'); updateSelBar(); return;
    }
    const { ok, status } = await api('/api/seats/select', { method: 'POST', body: { screeningId: S.screeningId, seatNos: [seatNo], requestId: S.rid } });
    if (ok) { S.mine.add(seatNo); cell.classList.add('mine'); updateSelBar(); }
    else if (status === 409) { cell.classList.add('taken'); cell.onclick = null; toast('이미 선점된 좌석입니다.', true); }
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
  // toggleSeat 진행 중엔 재렌더를 쉰다 — 응답 직전 그리드 교체로 내 좌석이 회색으로 죽는 경합 방지
  S.seatPollTimer = setInterval(() => { if (!$('screen-seats').classList.contains('hidden') && !toggleSeat._busy) loadSeats(); }, 5000);
}
function stopSeatPoll() { if (S.seatPollTimer) { clearInterval(S.seatPollTimer); S.seatPollTimer = null; } }

// ================= 5) 결제 =================
function toPay() {
  const arr = [...S.mine].sort(); if (!arr.length) return;
  stopSeatPoll();
  $('payScreening').textContent = S.screeningLabel;
  $('paySeats').textContent = arr.join(', ');
  $('payAmount').textContent = (arr.length * PRICE).toLocaleString();
  $('payStatus').classList.add('hidden'); show('pay');
}
async function confirmPay() {
  const arr = [...S.mine].sort();
  $('payStatus').classList.remove('hidden'); $('payConfirm').disabled = true;
  const idem = `${S.rid}:${S.screeningId}:${arr.join('-')}`;
  const { ok, status, data } = await api('/api/bookings', { method: 'POST', body: { screeningId: S.screeningId, seatNos: arr, requestId: S.rid, idempotencyKey: idem } });
  $('payConfirm').disabled = false; $('payStatus').classList.add('hidden');
  if (ok) { showDone(data); }
  else if (status === 409) {
    // 부분 충돌(일부 좌석만 만료/탈취) 시 아직 유효한 내 락이 고아로 남지 않게 먼저 반환.
    toast('좌석이 만료되었거나 이미 판매되었습니다.', true);
    await api('/api/seats/release', { method: 'POST', body: { screeningId: S.screeningId, seatNos: arr, requestId: S.rid } });
    openSeats(S.screeningId, S.screeningLabel);
  }
  else if (status === 403) { serverKicked(); }
  else toast('결제 실패', true);
}

// ================= 6) 완료 =================
function showDone(b) {
  feedLog(`나(${S.userLabel}) 예매 완료 — ${b.bookingId}`, 'me');
  $('doneNo').textContent = b.bookingId;
  $('doneScreening').textContent = S.screeningLabel;
  $('doneSeats').textContent = (b.seatNos || []).sort().join(', ');
  $('doneAmount').textContent = (b.price || 0).toLocaleString();
  // 세션 상태를 전부 정리 — 남겨두면 탭 복귀 시 즉시 폴링이 EXPIRED(예매 완료로 큐에서 빠짐)를 받아
  // 완료 화면에서 "시간 초과" 강퇴로 오인한다. 화면 값은 이미 그려져 있어 지워도 안전.
  resetSession(); show('done');
}

// ================= 네비게이션 =================
async function backFromSeats() {
  const arr = [...S.mine];
  if (arr.length) await api('/api/seats/release', { method: 'POST', body: { screeningId: S.screeningId, seatNos: arr, requestId: S.rid } });
  S.mine = new Set(); stopSeatPoll(); toScreenings();
}
async function goHome() {
  stopPolling();   // 이탈 절차 중 백그라운드 폴링(EXPIRED 감시)이 끼어들지 않게 먼저 정지
  await releaseAndLeave();   // 홈 = 예매 포기
  resetSession(); loadMovies();
}
function resetSession() { S.pollEpoch++; stopPolling(); stopSeatPoll(); stopActiveTimer(); S.active = false; S.mine = new Set(); S.screeningId = null; S.screeningLabel = null; S.movieId = null; S.movieTitle = null; localStorage.removeItem('cgv_flow'); }   // 세션 상태 전부 정리. epoch++로 남은 폴링 응답도 폐기

// ================= 가상 관객 엔진 + 실황 =================
// 가상 관객 = 가짜 requestId로 enter한 클라이언트. 상태 전이(대기→입장→예매→퇴장)는
// 봇 틱(3초 폴링)이 관찰하고, 스트립(점)과 실황 피드가 그 관찰을 그대로 그린다.
// 폴링이 곧 생존 신호라 틱이 멈추면 대기 관객은 30초 만에 서버가 회수한다.

function feedLog(msg, cls) {
  const t = new Date().toTimeString().slice(0, 8);
  const ul = $('feedList'); if (!ul) return;
  const empty = ul.querySelector('.qv-empty'); if (empty) empty.remove();
  // 새 항목만 prepend — 전체 재렌더 대신. 새 줄에만 슬라이드 인 애니메이션이 걸린다.
  const li = document.createElement('li'); li.className = cls || '';
  li.innerHTML = `<span>${t}</span>${esc(msg)}`;
  ul.prepend(li);
  while (ul.children.length > 60) ul.lastChild.remove();
}
function renderFeed() {
  const ul = $('feedList'); if (!ul) return;
  if (!ul.children.length) ul.innerHTML = '<li class="qv-empty">시스템 이벤트(승격·예매·회수)가 여기에 흐릅니다</li>';
}
function renderStrip() {
  const box = $('queueStrip'); if (!box) return;
  const cls = { lobby: 'd-lobby', waiting: 'd-wait', admitted: 'd-in', booked: 'd-book', gone: 'd-gone' };
  const name = { lobby: '대기실(오픈 대기)', waiting: '대기', admitted: '입장', booked: '예매 완료', gone: '퇴장' };
  box.innerHTML = S.fakes.length
    ? S.fakes.map(f => `<i class="d ${cls[f.state]}" title="관객-${f.label} · ${name[f.state]}"></i>`).join('')
    : '<span class="qv-empty">아직 가상 관객이 없습니다 — 위에서 넣어보세요</span>';
  $('qvCount').textContent = S.fakes.filter(f => f.state === 'lobby' || f.state === 'waiting' || f.state === 'admitted').length;
}

// 관객 생성 — 아직 서버에 보내지 않은 상태로 목록에만 올린다.
function createFakes(n, state) {
  const list = [];
  for (let i = 0; i < n; i++) {
    const rid = 'bot-' + uuid().slice(0, 8);
    const f = { rid, label: rid.slice(4), state, entered: false };
    S.fakes.push(f); list.push(f);
  }
  return list;
}

// 실제 줄 세우기 — 목록의 관객을 서버 대기열로 보낸다.
// 6개 단위 병렬: 동일 오리진 동시연결 한도 안에서, 원거리 RTT에서도 투입이 생존 창(30초)을 넘기지 않게.
async function enterFakes(list, label) {
  if (!list.length) return;
  const gen = S.simGen;
  const btn = $('heroAdd'), btnTxt = btn.textContent;
  btn.disabled = true;
  if (!S.simTimer) S.simTimer = setInterval(botTick, 3000);   // 투입 중에도 생존 폴링이 돌게 선가동
  let admitted = 0, waiting = 0, failed = 0;
  feedLog(`${label} ${list.length}명 줄 서는 중`, 'sys');
  try {
    const CHUNK = 6;
    for (let i = 0; i < list.length && gen === S.simGen; i += CHUNK) {
      const batch = list.slice(i, i + CHUNK).map(f =>
        api('/api/admission/enter', { method: 'POST', body: { movieId: S.simMovieId, requestId: f.rid } }).then(({ data }) => {
          if (gen !== S.simGen) return;
          if (data && (data.status === 'ADMITTED' || data.status === 'ALREADY_ACTIVE')) { f.state = 'admitted'; f.entered = true; admitted++; }
          else if (data && data.status === 'WAITING') { f.state = 'waiting'; f.entered = true; waiting++; }
          else { f.state = 'gone'; failed++; }   // 503·네트워크 실패를 입장으로 위장하지 않는다
        }));
      await Promise.all(batch);
      btn.textContent = `투입 중 ${Math.min(i + CHUNK, list.length)}/${list.length}`;
      renderStrip();
    }
  } finally { btn.disabled = false; btn.textContent = btnTxt; }
  if (gen !== S.simGen) return;
  feedLog(`줄 서기 완료 — 즉시 입장 ${admitted} · 대기 ${waiting}${failed ? ` · 실패 ${failed}` : ''}`, failed ? 'gone' : 'sys');
  renderStrip();
}

async function addFakes(n) {
  if (addFakes._busy) return;
  if (!S.simMovieId) { toast('상영 정보를 먼저 불러오세요.', true); return; }
  n = Math.max(1, Math.min(200, n || 5));
  // 탭당 활성 총량 200 상한 — 한 탭의 폴링이 생존 창(30초) 안에 전원을 돌 수 있는 규모.
  const activeCnt = S.fakes.filter(f => f.state === 'lobby' || f.state === 'waiting' || f.state === 'admitted').length;
  if (activeCnt + n > 200) { toast(`동시 최대 200명까지입니다 — 지금 ${activeCnt}명`, true); return; }

  const gs = gateState().s;
  if (gs === 'before') {
    // 오픈 전에는 줄을 세우지 않는다 — 미리 넣은 관객이 대기열 앞자리를 차지하면
    // 오픈 시각에 들어오는 사람(과 러시)이 이미 진 채로 시작한다. 대기실에 모아 두고
    // 오픈 순간에 러시와 함께 한꺼번에 내보낸다.
    createFakes(n, 'lobby');
    feedLog(`관객 ${n}명 대기실 입장 — 오픈 시각에 함께 줄을 섭니다`, 'sys');
    renderStrip(); applyGate();
    return;
  }
  if (gs === 'closed') { toast('예매가 마감돼 관객을 넣을 수 없습니다.', true); return; }

  addFakes._busy = true;
  try { await enterFakes(createFakes(n, 'waiting'), '가상 관객'); }
  finally { addFakes._busy = false; }
}

// 대기실 개방 — 모아 둔 관객과 오픈 러시를 같은 순간에 내보낸다(동등 출발).
async function releaseLobby(rushN) {
  if (addFakes._busy) return;
  const lobby = S.fakes.filter(f => f.state === 'lobby');
  // 러시 여유분은 전체 활성(대기실+대기+입장) 기준 — 대기실만 빼면 기존 대기 인원과 합쳐 200을 넘는다
  const activeCnt = S.fakes.filter(f => f.state === 'lobby' || f.state === 'waiting' || f.state === 'admitted').length;
  const rush = createFakes(Math.min(rushN || 0, Math.max(0, 200 - activeCnt)), 'waiting');
  if (!lobby.length && !rush.length) return;
  addFakes._busy = true;
  try { await enterFakes([...lobby, ...rush], '오픈 러시'); }
  finally { addFakes._busy = false; }
}

// 마감 처리 — 아직 줄에 남아 있던 관객을 전부 내보낸다(오픈 창이 닫히면 대기도 끝).
async function kickWaiting() {
  const stuck = S.fakes.filter(f => f.state === 'waiting' || f.state === 'lobby');
  stuck.forEach(f => {
    if (f.entered) api('/api/admission/leave', { method: 'POST', body: { movieId: S.simMovieId, requestId: f.rid } });
    f.state = 'gone';
  });
  if (stuck.length) feedLog(`마감 — 대기 중이던 ${stuck.length}명 퇴장`, 'gone');
  renderStrip();
  // 나도 대기 중이었다면 같이 나간다. 입장한 뒤라면 세션은 그대로 둔다(들어간 사람은 마저 예매).
  if (!$('screen-waiting').classList.contains('hidden')) {
    toast('예매가 마감되어 대기열에서 나왔습니다.', true);
    await goHome();
  }
}
// 봇 틱(3초): 가상 관객들도 /position을 폴링한다 — 폴링 세계의 생존 신호.
// ① 대기 관객: 폴링 = lastseen 도장 → waiting 타임아웃(30s)에 안 쓸려나가고 줄 유지.
// ② 입장 관객: 자동 예매 ON이면 좌석 잡고 예매하고 나감(자리 순환). OFF면 세션 타임아웃까지 점유.
// 상태 전이를 여기서 감지해 실황 피드에 남긴다.
async function botTick() {
  if (botTick._busy) return;   // 재진입 가드 — 관객 많을 때 틱(3s)이 겹쳐 중복 API 레이스 방지
  botTick._busy = true;
  const gen = S.simGen;
  // 오픈 전·마감 후엔 자동 예매를 쉰다 — 오픈 전엔 줄만 쌓이는 것이 실제 티케팅과 같다.
  const gs = gateState().s;
  const canBook = $('botMode').checked && (gs === 'none' || gs === 'open');
  try {
  // ① 생존 폴링 — 6개 단위 병렬(순차면 관객 수 × RTT가 생존 창 30초를 넘길 수 있다)
  const live = S.fakes.filter(f => f.entered && f.state !== 'booked' && f.state !== 'gone');
  for (let i = 0; i < live.length && gen === S.simGen; i += 6) {
    await Promise.all(live.slice(i, i + 6).map(async f => {
      const p = await api(`/api/admission/position?movieId=${encodeURIComponent(S.simMovieId)}&requestId=${encodeURIComponent(f.rid)}`);
      if (gen !== S.simGen || !p.ok || !p.data) return;
      if (p.data.status === 'EXPIRED') { f.state = 'gone'; feedLog(`관객-${f.label} 퇴장 — 세션 만료·이탈`, 'gone'); return; }
      if (p.data.status !== 'ADMITTED') return;    // WAITING: 도장 찍었으니 줄 유지
      if (f.state === 'waiting') { f.state = 'admitted'; feedLog(`관객-${f.label} 승격 → 입장`, 'in'); }
    }));
  }
  // ② 자동 예매 여정 — 입장자는 정원(2)만큼이라 순차로 충분
  if (canBook) {
    for (const f of S.fakes) {
      if (gen !== S.simGen) break;
      if (f.state !== 'admitted') continue;
      const sc = await api(`/api/screenings?movieId=${encodeURIComponent(S.simMovieId)}&requestId=${encodeURIComponent(f.rid)}`);
      if (!sc.ok || !sc.data) continue;            // 입장 직후 Kafka 갭이면 403 → 다음 틱 재시도
      const avail = sc.data.find(s => s.remain > 0);
      if (!avail) { f.state = 'gone'; feedLog(`관객-${f.label} 퇴장 — 전 회차 매진`, 'gone'); continue; }
      const seats = await api(`/api/seats?screeningId=${avail.screeningId}&requestId=${encodeURIComponent(f.rid)}`);
      const free = (seats.data || []).find(s => !s.taken); if (!free) continue;
      const sel = await api('/api/seats/select', { method: 'POST', body: { screeningId: avail.screeningId, seatNos: [free.seatNo], requestId: f.rid } });
      if (!sel.ok) continue;
      const bk = await api('/api/bookings', { method: 'POST', body: { screeningId: avail.screeningId, seatNos: [free.seatNo], requestId: f.rid, idempotencyKey: `${f.rid}:${avail.screeningId}:${free.seatNo}` } });
      if (bk.ok) {
        f.state = 'booked';
        feedLog(`관객-${f.label} ${avail.branch} ${avail.screenNo}관 ${free.seatNo} 예매 완료 → 자리 반환`, 'book');
      } else {
        // 예매 실패 — 잡은 좌석·자리 반납(release+leave 짝) 후 종료. 안 하면 TTL까지 유령 점유.
        await api('/api/seats/release', { method: 'POST', body: { screeningId: avail.screeningId, seatNos: [free.seatNo], requestId: f.rid } });
        await api('/api/admission/leave', { method: 'POST', body: { movieId: S.simMovieId, requestId: f.rid } });
        f.state = 'gone'; feedLog(`관객-${f.label} 예매 실패 → 퇴장`, 'gone');
      }
    }
  }
  renderStrip();
  } finally { botTick._busy = false; }
}
async function clearFakes() {
  S.simGen++;   // 진행 중인 투입·틱을 즉시 무효화 — "내보냈는데 계속 늘어남" 방지
  let n = 0;
  for (const f of S.fakes) {
    if (f.state === 'lobby') { n++; continue; }   // 서버에 없던 관객 — 목록 제거만으로 끝
    if (f.entered && (f.state === 'waiting' || f.state === 'admitted')) { n++; api('/api/admission/leave', { method: 'POST', body: { movieId: S.simMovieId, requestId: f.rid } }); }
  }
  S.fakes = []; clearInterval(S.simTimer); S.simTimer = null;
  feedLog(`내 가상 관객 ${n}명 내보냄`, 'sys'); renderStrip();
}

// ================= 오픈 게이트 + 예약 투입 (콘솔 기능) =================
// 서버는 이 시각들을 모른다 — 게이트는 화면(예매 버튼)만 잠그고, 예약 투입은 이 탭이
// 시각이 되면 가상 관객을 넣는다. 둘 다 localStorage라 브라우저마다 독립이다.
// 오픈 시각과 예약 투입을 같은 시각으로 걸면 "오픈 순간 N명 폭주"가 재현된다.
const OPEN_WINDOW_MS = 10 * 60 * 1000;   // 오픈 후 예매 가능 창 = 10분

function openAtMs() { const v = parseInt(localStorage.getItem('cgv_open_at_ms') || '0'); return v > 0 ? v : 0; }
function openRushN() { return Math.min(200, parseInt(localStorage.getItem('cgv_open_n') || '0')); }
// 게이트 상태: none(예약 없음 — 상시 열림) / before(오픈 전) / open(오픈 창) / closed(창 종료)
function gateState() {
  const at = openAtMs();
  if (!at) return { s: 'none' };
  const now = Date.now();
  if (now < at) return { s: 'before', remain: at - now };
  if (now < at + OPEN_WINDOW_MS) return { s: 'open', remain: at + OPEN_WINDOW_MS - now };
  if (now < at + OPEN_WINDOW_MS + 60000) return { s: 'closed' };   // 마감 표시는 1분만
  // 지난 예약은 자동 소멸 — 재방문자가 죽은 '예매 마감' 화면을 만나지 않게(발동 기록은 openHist에 남아 있다)
  ['cgv_open_at_ms', 'cgv_open_n', 'cgv_open_fired'].forEach(k => localStorage.removeItem(k));
  return { s: 'none' };
}
function fmtHMS(ms) {
  const s = Math.ceil(ms / 1000);
  return `${String(Math.floor(s / 3600)).padStart(2, '0')}:${String(Math.floor(s % 3600 / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
}
const fmtAt = (ms) => {
  const d = new Date(ms);
  return `${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
};

// 오픈 기록 — 발동된 오픈을 최근 5건 보관(localStorage)
function histPush(at, n) {
  let h = []; try { h = JSON.parse(localStorage.getItem('cgv_open_history') || '[]'); } catch {}
  h.unshift({ at, n }); h = h.slice(0, 5);
  localStorage.setItem('cgv_open_history', JSON.stringify(h));
  renderHist();
}
function renderHist() {
  const el = $('openHist'); if (!el) return;
  let h = []; try { h = JSON.parse(localStorage.getItem('cgv_open_history') || '[]'); } catch {}
  el.innerHTML = h.map(e => `<li>${fmtAt(e.at)} 오픈${e.n > 0 ? ` · ${e.n}명 러시` : ' · 게이트만'}</li>`).join('');
}

function applyGate() {
  const st = gateState();
  // 예매 버튼(이벤트 배너) — 클래스 토글로(버튼의 btn 클래스 보존)
  document.querySelectorAll('.go').forEach(el => {
    el.classList.remove('locked', 'closed');
    if (st.s === 'before') { el.classList.add('locked'); el.textContent = `오픈까지 ${fmtHMS(st.remain)}`; el.disabled = true; }
    else if (st.s === 'closed') { el.classList.add('closed'); el.textContent = '예매 마감'; el.disabled = true; }
    else { el.textContent = '예매하기 →'; el.disabled = false; }
  });
  // 콘솔 예약 줄 = 예약 "내용"만. 진행 상태·카운트다운은 시계 블록이 단일 출처.
  const note = $('openNote');
  if (note) {
    if (st.s === 'before') {
      const lobbyN = S.fakes.filter(f => f.state === 'lobby').length;
      const total = openRushN() + lobbyN;
      note.textContent = `다음 오픈 ${fmtAt(openAtMs())}`
        + (total > 0 ? ` · 오픈 시 ${total}명 동시 진입(대기실 ${lobbyN} + 러시 ${openRushN()})` : '');
    }
    else if (st.s === 'open') note.textContent = `진행 중 오픈 ${fmtAt(openAtMs())} — 마감 후 재예약 가능`;
    else if (st.s === 'closed') note.textContent = '지난 오픈이 마감됐습니다 — 해제 후 다시 예약하세요';
    else note.textContent = '';
  }
  // 진행 중엔 예약 변경 잠금 — 진행 중이던 오픈이 안내 없이 취소되는 것 방지
  const setBtn = $('openSet');
  if (setBtn) setBtn.disabled = (st.s === 'open');
  // 시계 블록 — 상태의 단일 출처. 오픈이 다가올수록 달아오른다.
  const box = document.querySelector('.con-clock'), sub = $('clockSub');
  if (box) {
    box.classList.remove('warn', 'hot', 'live');
    let subTxt = '';
    if (st.s === 'before') {
      if (st.remain <= 10000) box.classList.add('hot');
      else if (st.remain <= 60000) box.classList.add('warn');
      subTxt = `오픈까지 ${fmtHMS(st.remain)}`;
    } else if (st.s === 'open') { box.classList.add('live'); subTxt = `● 예매 진행 중 · 마감까지 ${fmtHMS(st.remain)}`; }
    else if (st.s === 'closed') subTxt = '오픈 마감됨';
    if (sub) sub.textContent = subTxt;
  }
  // 상태 전이 — 오픈(러시 발동)과 마감. 발동 조건은 전이가 아니라 "open인데 아직 미발동"이라
  // 오픈 창 도중 새로고침해도 fired 표식만 없으면 발동된다.
  const prev = applyGate._st || 'none';
  if (st.s === 'open') {
    const at = String(openAtMs());
    if (localStorage.getItem('cgv_open_fired') !== at) {
      localStorage.setItem('cgv_open_fired', at);
      feedLog('예매가 열렸습니다 — 입장 시작', 'sys');
      if (prev === 'before') toast('예매가 열렸습니다');
      const n = openRushN();
      const lobbyN = S.fakes.filter(f => f.state === 'lobby').length;
      const fresh = Date.now() - openAtMs() < 120000;   // 오픈 한참 뒤 발견한 예약은 투입 생략
      histPush(openAtMs(), fresh ? n + lobbyN : 0);     // 기록은 실제 발동 기준
      if (fresh && (n > 0 || lobbyN > 0)) {
        feedLog(`오픈 — 대기실 ${lobbyN}명 + 러시 ${n}명이 동시에 줄을 섭니다`, 'sys');
        // 즉시 투입이 돌고 있으면 몇 초 물러서서 재시도 — 러시가 무음 소실되지 않게
        const tryRush = (left) => { if (!addFakes._busy) releaseLobby(n); else if (left > 0) setTimeout(() => tryRush(left - 1), 2000); };
        tryRush(5);
      }
    }
  }
  if (prev === 'open' && st.s === 'closed') {
    feedLog('예매가 마감됐습니다 — 오픈 창(10분) 종료', 'sys');
    toast('예매가 마감됐습니다');
    kickWaiting();   // 줄에 남은 관객은 마감과 함께 나간다
  }
  applyGate._st = st.s;
}

// 오픈 슬롯 — 데모용 즉석 2개 + 30분 단위 정규 슬롯 8개
function slotOptions() {
  const out = [
    { label: '지금 +1분 (데모)', ms: Date.now() + 60000 },
    { label: '지금 +3분 (데모)', ms: Date.now() + 180000 },
  ];
  const d = new Date(); d.setSeconds(0, 0);
  d.setMinutes(d.getMinutes() < 30 ? 30 : 60);
  for (let i = 0; i < 8; i++) {
    out.push({ label: `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')} 오픈`, ms: d.getTime() });
    d.setMinutes(d.getMinutes() + 30);
  }
  return out;
}
function fillSlots() {
  const sel = $('openSlot'); if (!sel) return;
  const cur = sel.value;
  sel.innerHTML = slotOptions().map(o => `<option value="${o.ms}">${o.label}</option>`).join('');
  if ([...sel.options].some(op => op.value === cur)) sel.value = cur;
}

// 투입·러시 인원 프리셋 — 자유 입력 대신 검증된 범위만 고르게 한다.
// 상한 200 = 브라우저 생성기의 한계(동시연결 6개·생존 창 30초 폴링)이지 백엔드 정원이 아니다.
// 그 이상의 부하 실측은 k6 몫. 상한 조정은 이 목록만 바꾸면 된다.
const INJECT_PRESETS = [5, 10, 30, 50, 100, 200];
const RUSH_PRESETS = [0, 10, 50, 100, 200];

// 콘솔 컨트롤 초기화 — 오픈 예약·기본값·접기
(function consoleInit() {
  fillSlots();
  const sel = $('openSlot'), openN = $('openN');
  sel.onmousedown = fillSlots;   // 드롭다운 여는 순간 데모 슬롯(+1분 등)을 현재 기준으로 재계산
  $('heroN').innerHTML = INJECT_PRESETS.map(v => `<option value="${v}"${v === 30 ? ' selected' : ''}>${v}명</option>`).join('');
  if (openN) {
    // 저장된 러시 인원이 프리셋에 없으면 가장 가까운 값으로 스냅
    const cur = openRushN() || 50;
    const snap = RUSH_PRESETS.reduce((a, b) => Math.abs(b - cur) < Math.abs(a - cur) ? b : a);
    openN.innerHTML = RUSH_PRESETS.map(v => `<option value="${v}"${v === snap ? ' selected' : ''}>${v ? `${v}명 러시` : '러시 없음'}</option>`).join('');
  }
  // 첫 방문 기본값 — 3분 뒤 오픈 + 50명 러시. 방문자가 머무는 동안 오픈이 실제로 발동해
  // "곧 오픈" 연출이 데모로 이어진다. (30분 슬롯 시드는 첫 CTA를 30분 잠그는 함정이라 안 쓴다.)
  if (!openAtMs() && !localStorage.getItem('cgv_open_seeded')) {
    localStorage.setItem('cgv_open_seeded', '1');
    localStorage.setItem('cgv_open_at_ms', String(slotOptions()[1].ms));
    localStorage.setItem('cgv_open_n', '50');
  }
  $('openSet').onclick = () => {
    const at = parseInt(sel.value) || 0;
    if (at <= Date.now()) { toast('지난 시각입니다 — 슬롯을 다시 선택하세요.', true); fillSlots(); return; }
    const n = Math.max(0, Math.min(200, parseInt(openN.value) || 0));
    localStorage.setItem('cgv_open_at_ms', String(at));
    localStorage.setItem('cgv_open_n', String(n));
    localStorage.removeItem('cgv_open_fired');
    feedLog(`예매 오픈 예약 — ${fmtAt(at)}${n > 0 ? ` · 오픈 시 ${n}명 러시` : ''}`, 'sys');
    applyGate();
  };
  $('openClear').onclick = () => {
    if (!openAtMs()) return;   // 예약이 없으면 할 일 없음 — 연타로 실황에 해제 로그가 도배되지 않게
    ['cgv_open_at_ms', 'cgv_open_n', 'cgv_open_fired'].forEach(k => localStorage.removeItem(k));
    feedLog('오픈 예약 해제', 'sys');
    applyGate();
  };
  const body = $('conBody'), tg = $('conToggle');
  const setCon = (open) => { body.classList.toggle('hidden', !open); tg.textContent = open ? '접기' : '펴기'; localStorage.setItem('cgv_console', open ? '1' : '0'); };
  tg.onclick = () => setCon(body.classList.contains('hidden'));
  if (localStorage.getItem('cgv_console') === '0') setCon(false);
  renderHist();
})();

// ?ops — 운영자 도구. 쿼리 파라미터는 화면을 여는 스위치일 뿐 잠금이 아니다.
// 실제 판정은 서버가 X-Admin-Token으로 한다(토큰 없는 배포엔 그 API 자체가 없다).
(function ops() {
  if (!new URLSearchParams(location.search).has('ops')) return;
  $('opsBox').classList.remove('hidden');
  const msg = $('opsMsg'), btn = $('opsReset');
  btn.onclick = async () => {
    const token = $('opsToken').value.trim();
    if (!token) { msg.textContent = '토큰을 입력하세요.'; return; }
    if (!S.simMovieId) { msg.textContent = '상영 정보를 아직 불러오지 못했습니다 — 잠시 후 다시 시도하세요.'; return; }
    if (!confirm('예매 기록과 대기열을 전부 지웁니다. 계속할까요?')) return;
    btn.disabled = true; msg.textContent = '초기화 중…';
    const h = { 'Content-Type': 'application/json', 'X-Admin-Token': token };
    // 대기열 먼저 — 예매를 지우는 동안 새로 승격된 관객이 좌석을 잡아 잔재가 남지 않게.
    const q = await fetch('/api/admission/reset', { method: 'POST', headers: h, body: JSON.stringify({ movieId: S.simMovieId }) }).catch(() => null);
    const b = await fetch('/api/admin/reset', { method: 'POST', headers: h }).catch(() => null);
    btn.disabled = false;
    if (!q || !b || !q.ok || !b.ok) {
      const code = [q && q.status, b && b.status].filter(Boolean).join('/');
      msg.textContent = `초기화 실패 (${code || '연결 불가'}) — 토큰이나 서버 상태를 확인하세요.`;
      return;
    }
    const info = await b.json().catch(() => ({}));
    msg.textContent = `초기화 완료 — 예매 ${info.bookingsDeleted ?? 0}건 삭제, 방송일 ${(info.broadcastAt || '').slice(0, 16).replace('T', ' ')}`;
    clearFakes(); feedLog('데이터 초기화 — 예매·대기열을 비웠습니다', 'sys');
    loadMovies();
  };
})();

// ================= 전체 현황 (GET /stats — 시스템 전체 숫자) =================
async function pollStats() {
  if (!S.simMovieId) return;
  const { ok, data } = await api(`/api/admission/stats?movieId=${encodeURIComponent(S.simMovieId)}`);
  const bar = $('statsBar');
  if (!ok || !data) {
    // 실패 시 직전 값 유지(플리커 방지) — 다만 연속 실패면 낡은 값임을 흐림으로 표시
    pollStats._fails = (pollStats._fails || 0) + 1;
    if (bar && pollStats._fails >= 3) bar.classList.add('stale');
    return;
  }
  pollStats._fails = 0; if (bar) bar.classList.remove('stale');
  // 정원(capacity)은 화면에 노출하지 않는다 — 실서비스가 정원을 광고하지 않는 것과 같게.
  // "바로 입장" 판정에만 내부적으로 쓴다. 게이트 상태가 우선한다(오픈 전·마감엔 입장 자체가 안 됨).
  const gs = gateState().s;
  const canEnterNow = data.active < data.capacity && data.waiting === 0 && (gs === 'none' || gs === 'open');
  let etaVal;
  if (gs === 'before') etaVal = '오픈 전';
  else if (gs === 'closed') etaVal = '마감';
  else etaVal = canEnterNow ? '바로 입장' : fmtEta(data.etaNextSeconds ?? -1);
  // 오픈 전에는 서버 대기열이 비어 있는 것이 정상이다(줄은 오픈 순간에 선다).
  // 그 구간에는 대신 대기실에 모인 인원 — 오픈을 기다리는 접속자 수 — 을 보여준다.
  const vals = gs === 'before'
    ? [['대기실 인원', S.fakes.filter(f => f.state === 'lobby').length], ['입장까지 예상', etaVal]]
    : [['전체 대기', data.waiting], ['입장까지 예상', etaVal]];
  if (bar) {
    bar.innerHTML = vals.map(([k, v]) => `<span class="stat"><em>${k}</em><b>${v}</b></span>`).join('');
    // 값이 바뀐 타일에 펄스 — 화면이 살아 있다는 신호
    const prev = pollStats._prev || [];
    vals.forEach(([, v], i) => {
      if (prev[i] !== undefined && prev[i] !== v) {
        const b = bar.children[i] && bar.children[i].querySelector('b');
        if (b) { b.classList.add('bump'); setTimeout(() => b.classList.remove('bump'), 500); }
      }
    });
    pollStats._prev = vals.map(([, v]) => v);
  }
}
setInterval(pollStats, 3000);

// ================= 바인딩 =================
$('homeBtn').onclick = goHome;
$('waitLeave').onclick = goHome;
$('seatsBack').onclick = backFromSeats;
$('toPay').onclick = toPay;
$('payBack').onclick = () => openSeats(S.screeningId, S.screeningLabel, true);   // 내 좌석 유지
$('payConfirm').onclick = confirmPay;
$('doneHome').onclick = () => { resetSession(); loadMovies(); };
$('heroAdd').onclick = () => addFakes(parseInt($('heroN').value));
$('clearFakes').onclick = clearFakes;
$('idHintBtn').onclick = login;
$('botMode').onchange = () => feedLog(`자동 예매 ${$('botMode').checked ? 'ON — 입장한 관객이 예매하고 나간다(자리 순환)' : 'OFF — 입장한 관객이 자리를 점유한다(세션 만료까지)'}`, 'sys');
// intro는 첫 방문에 펼쳐 보여준다(핵심 설득 콘텐츠 — 스토리·아키텍처·GitHub). 접은 선택은 기억.
if (!localStorage.getItem('cgv_intro_closed')) $('introPanel').classList.remove('hidden');
$('introToggle').onclick = () => {
  const closed = $('introPanel').classList.toggle('hidden');
  localStorage.setItem('cgv_intro_closed', closed ? '1' : '');
};

// 상단 시계 — 오픈 시각 게이트가 생기면 기준 시계가 된다. 대기 화면의 생존 신호 표시도 여기서 그린다.
(function clock() {
  const el = $('clock'); if (!el) return;
  const tick = () => {
    el.textContent = new Date().toTimeString().slice(0, 8);
    applyGate();    // 오픈 카운트다운·버튼 잠금·오픈 러시(1초)
    const h = $('waitHeart');
    if (h && S.lastPollAt && !$('screen-waiting').classList.contains('hidden')) {
      h.textContent = `폴링 = 생존 신호 · 마지막 신호 ${Math.max(0, Math.round((Date.now() - S.lastPollAt) / 1000))}초 전`;
    }
  };
  tick(); setInterval(tick, 1000);
})();

// ================= 시작: 로그인 복구 =================
(function init() {
  try { const a = JSON.parse(localStorage.getItem('cgv_auth') || 'null'); if (a && a.rid) { S.rid = a.rid; S.userLabel = a.label; } } catch {}
  renderAuth();
  loadMovies();
  renderStrip(); renderFeed();   // 빈 상태 문구부터 표시(대기열 뷰는 상시 노출)
  // 새로고침 복구 — 대기/입장 진행 중이었으면 같은 requestId로 enter 재호출.
  // 서버: active=ALREADY_ACTIVE(유지) / waiting=꼬리로 밀림. 로그인 상태에서만.
  try {
    const flow = JSON.parse(localStorage.getItem('cgv_flow') || 'null');
    if (S.rid && flow && flow.movieId) {
      if (flow.activeUntil) S.resumeUntil = flow.activeUntil;   // 세션 카운트다운 이어받기
      enter(flow.movieId, flow.title);
    }
  } catch {}
})();
