/* CGV 예매 프론트 — 백엔드서비스-올인원 §1-8·§3-1·§3-4·§2-7 기준.
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
  simMovieId: null, fakes: [], simTimer: null,// 시뮬레이터
};

// ================= DOM/화면 =================
const $ = (id) => document.getElementById(id);
const screens = ['movies','waiting','entering','screenings','seats','pay','done'];
const ACTIVE_SCREENS = ['entering','screenings','seats','pay'];   // 카운트다운 보일 화면
function show(name) {
  screens.forEach(s => $('screen-' + s).classList.toggle('hidden', s !== name));
  $('activeTimer').classList.toggle('hidden', !(ACTIVE_SCREENS.includes(name) && S.activeUntil));
  $('sim').classList.toggle('hidden', name !== 'movies');   // 시뮬레이터는 홈(영화목록)에서만
}
function toast(msg, err) {
  const t = $('toast'); t.textContent = msg; t.className = 'toast' + (err ? ' err' : '');
  clearTimeout(toast._t); toast._t = setTimeout(() => t.classList.add('hidden'), 2400);
}

// ================= API =================
async function api(path, opts = {}) {
  // fetch 자체 예외(커넥션 거부·순단 — 백엔드 재시작 중 흔함)도 잡는다(리뷰 F1).
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
  const a = $('authArea');
  if (S.rid) {
    a.innerHTML = `<span class="who"><i>${esc(S.userLabel)}</i> 님</span><button class="btn small ghost" id="logoutBtn">로그아웃</button>`;
    $('logoutBtn').onclick = logout;
  } else {
    a.innerHTML = `<button class="btn small primary" id="loginBtn">로그인</button>`;
    $('loginBtn').onclick = login;
  }
}
function login() {
  const cnt = (parseInt(localStorage.getItem('cgv_ucount') || '0') + 1);
  localStorage.setItem('cgv_ucount', String(cnt));
  S.rid = uuid(); S.userLabel = 'user' + cnt;
  localStorage.setItem('cgv_auth', JSON.stringify({ rid: S.rid, label: S.userLabel }));
  renderAuth(); toast(`${S.userLabel} 로그인`);
}
async function logout() {
  stopPolling();   // 이탈 절차 중 백그라운드 폴링이 끼어들지 않게 먼저 정지(F8)
  // 잡은 좌석 락 먼저 반환 — goHome과 같은 짝 연산(select↔release). 안 하면 45초 유령 점유(F3).
  if (S.mine.size && S.screeningId && S.rid) {
    await api('/api/seats/release', { method: 'POST', body: { screeningId: S.screeningId, seatNos: [...S.mine], requestId: S.rid } });
  }
  if (S.rid && S.movieId) { await api('/api/admission/leave', { method: 'POST', body: { movieId: S.movieId, requestId: S.rid } }); }
  resetSession();
  S.rid = null; S.userLabel = null;
  localStorage.removeItem('cgv_auth');
  renderAuth(); loadMovies(); toast('로그아웃되었습니다');
}
function requireLogin() { if (!S.rid) { toast('로그인 후 이용해주세요.', true); return false; } return true; }

// ================= 1) 영화 목록 =================
async function loadMovies() {
  stopActiveTimer();
  show('movies');
  const { ok, data } = await api('/api/movies');
  const list = $('movieList'); list.innerHTML = '';
  if (!ok || !data || !data.length) { list.innerHTML = '<p style="color:#9a9aa8">상영 정보를 불러오지 못했습니다.</p>'; return; }
  S.simMovieId = data[0].id;   // 시뮬레이터가 쓸 영화
  data.forEach(m => {
    const time = m.broadcastAt ? m.broadcastAt.replace('T', ' ').slice(0, 16) : '';
    const card = document.createElement('div');
    card.className = 'movie-card';
    card.innerHTML =
      `<div class="movie-poster"><span class="poster-live">● LIVE</span><span class="poster-emoji">⚾</span><span class="poster-cap">KBO ALL-STAR</span></div>` +
      `<div class="movie-body"><span class="badge">단독 생중계</span><h3>${esc(m.title)}</h3><div class="time">${time}</div><div class="go">예매하기 →</div></div>`;
    card.onclick = () => { if (requireLogin()) enter(m.id, m.title); };   // 비로그인 차단
    list.appendChild(card);
  });
}

// ================= enter =================
async function enter(movieId, title) {
  if (enter._busy) return;   // 연타 가드 — 같은 requestId로 enter 중복 POST 방지(F5)
  enter._busy = true;
  try {
    S.movieId = movieId; S.movieTitle = title;
    // 새로고침 복구용 — 대기/입장 진행 중임을 저장. init()이 F5 후 이걸로 enter 재호출.
    // → 서버가 같은 requestId를 보고 active=유지 / waiting=꼬리로 밀기(§1-1).
    localStorage.setItem('cgv_flow', JSON.stringify({ movieId, title }));
    const { ok, status, data } = await api('/api/admission/enter', { method: 'POST', body: { movieId, requestId: S.rid } });
    // 서버의 정직한 실패(Kafka 발행 실패 → 보상 롤백, queue §3-A): 에러로 깨지지 말고 지연 안내.
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
      show('waiting'); startPolling();         // SSE 대신 GET /position 폴링(§1-2)
    } else {                                   // 200 ADMITTED → active
      goActive();
    }
  } finally { enter._busy = false; }
}

// ================= 2) 대기 폴링 (GET /position, §1-2) =================
// 서버 push(SSE) 없음. 클라가 주기적으로 내 순번·입장여부를 pull.
// 응답 3-state: WAITING(순번·뒤인원·예상) / ADMITTED(입장) / EXPIRED(타임아웃·이탈).
// active 진입 후에도 느린 폴링 유지 → 서버 세션 타임아웃(EXPIRED) 감지(SSE TIMEOUT 대체).
function startPolling() { stopPolling(); pollPosition(); }
function stopPolling() { if (S.pollTimer) { clearTimeout(S.pollTimer); S.pollTimer = null; } }
function scheduleNext(ms) { stopPolling(); S.pollTimer = setTimeout(pollPosition, ms + Math.random() * 300); } // jitter(§3-3)

async function pollPosition() {
  if (!S.rid || !S.movieId) return;
  const { ok, data } = await api(`/api/admission/position?movieId=${encodeURIComponent(S.movieId)}&requestId=${encodeURIComponent(S.rid)}`);
  if (!ok || !data) { scheduleNext(1000); return; }   // 일시 오류 → 재시도

  if (data.status === 'ADMITTED') {
    if (!S.active) goActive();        // 대기→입장 전환(최초 1회)
    else scheduleNext(5000);          // 이미 active면 EXPIRED 감시용 느린 폴링
    return;
  }
  if (data.status === 'EXPIRED') { serverKicked(); return; }   // 큐에서 빠짐(타임아웃/이탈/세션만료)

  // WAITING — 순번·뒤인원·예상시간 갱신 + 적응형 주기(§3-5)
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
function serverKicked() {      // EXPIRED = 진짜 퇴장(세션 타임아웃/이탈)
  toast('입장 시간이 초과되어 자리가 반환되었습니다.', true);
  resetSession(); loadMovies();
}

// ================= active 1분 카운트다운 (입장자만) =================
function startActiveTimer() {
  S.activeUntil = Date.now() + SESSION_SEC * 1000;
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

// ================= 3) 회차 선택 (403 재시도, §2-7) =================
async function toScreenings() {
  show('entering');
  for (let i = 0; i < 12; i++) {
    const { ok, status, data } = await api(`/api/screenings?movieId=${encodeURIComponent(S.movieId)}&requestId=${encodeURIComponent(S.rid)}`);
    if (ok) { renderScreenings(data || []); return; }   // 빈 바디 방어(F12)
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
  // 서버 응답(taken)에 소유자 구분이 없어 내 좌석이 회색으로 죽어버림(리뷰 F2).
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
  (data || []).forEach(s => { S.seatMeta[s.seatNo] = { row: s.row, col: s.col }; (rows[s.row] = rows[s.row] || []).push(s); });   // 빈 바디 방어(F12)
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
  if (toggleSeat._busy) return;   // 연타 가드 — 내 두 번째 요청이 409를 받아 내 좌석이 회색으로 죽는 것 방지(F6)
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
  S.seatPollTimer = setInterval(() => { if (!$('screen-seats').classList.contains('hidden')) loadSeats(); }, 5000);
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
    // 부분 충돌(일부 좌석만 만료/탈취) 시 아직 유효한 내 락이 고아로 남지 않게 먼저 반환(F4).
    toast('좌석이 만료되었거나 이미 판매되었습니다.', true);
    await api('/api/seats/release', { method: 'POST', body: { screeningId: S.screeningId, seatNos: arr, requestId: S.rid } });
    openSeats(S.screeningId, S.screeningLabel);
  }
  else if (status === 403) { serverKicked(); }
  else toast('결제 실패', true);
}

// ================= 6) 완료 =================
function showDone(b) {
  $('doneNo').textContent = b.bookingId;
  $('doneScreening').textContent = S.screeningLabel;
  $('doneSeats').textContent = (b.seatNos || []).sort().join(', ');
  $('doneAmount').textContent = (b.price || 0).toLocaleString();
  stopPolling(); stopSeatPoll(); stopActiveTimer(); localStorage.removeItem('cgv_flow'); show('done');
}

// ================= 네비게이션 =================
async function backFromSeats() {
  const arr = [...S.mine];
  if (arr.length) await api('/api/seats/release', { method: 'POST', body: { screeningId: S.screeningId, seatNos: arr, requestId: S.rid } });
  S.mine = new Set(); stopSeatPoll(); toScreenings();
}
async function goHome() {
  stopPolling();   // 이탈 절차 중 백그라운드 폴링(EXPIRED 감시)이 끼어들지 않게 먼저 정지(F8)
  const cur = screens.find(s => !$('screen-' + s).classList.contains('hidden'));
  // 홈 = 예매 포기(§3-4 네비게이션): 잡은 좌석 락 먼저 해제(안 하면 TTL 45초까지 유령 점유) → leave(자리 반납).
  if (S.mine.size && S.screeningId && S.rid) {
    await api('/api/seats/release', { method: 'POST', body: { screeningId: S.screeningId, seatNos: [...S.mine], requestId: S.rid } });
  }
  if (['screenings','seats','pay','waiting','entering'].includes(cur) && S.rid && S.movieId) {
    await api('/api/admission/leave', { method: 'POST', body: { movieId: S.movieId, requestId: S.rid } });
  }
  resetSession(); loadMovies();
}
function resetSession() { stopPolling(); stopSeatPoll(); stopActiveTimer(); S.active = false; S.mine = new Set(); S.screeningId = null; S.screeningLabel = null; S.movieId = null; S.movieTitle = null; localStorage.removeItem('cgv_flow'); }   // 세션 상태 전부 정리(F7)

// ================= 시연용 대기열 생성기 =================
// 가짜 requestId로 enter를 여러 번 → waiting/active 채움. 봇 옵션이면 admitted 시 자동 예매(자리 순환).
async function simAdd() {
  if (!S.simMovieId) { toast('영화 목록을 먼저 불러오세요.', true); return; }
  const n = Math.max(1, Math.min(50, parseInt($('simN').value) || 5));
  $('simAdd').disabled = true;
  let admitted = 0, waiting = 0;
  for (let i = 0; i < n; i++) {
    const rid = 'bot-' + uuid().slice(0, 8);
    S.fakes.push({ rid, done: false });
    const { data } = await api('/api/admission/enter', { method: 'POST', body: { movieId: S.simMovieId, requestId: rid } });
    if (data && data.status === 'WAITING') waiting++; else admitted++;
    await sleep(50);   // 브라우저 동시연결 제한 회피
  }
  $('simAdd').disabled = false;
  simStatus();
  if (!S.simTimer) S.simTimer = setInterval(botTick, 3000);
  toast(`가짜 대기자 ${n}명 투입 (입장 ${admitted} · 대기 ${waiting})`);   // 추가 알림
}
function simStatus() {
  const live = S.fakes.filter(f => !f.done).length;
  $('simStatus').textContent = `가짜 대기자 ${live}명` + (($('simBot').checked) ? ' · 봇 자동예매 ON' : '');
}
// 봇 틱(3초): 가짜들도 /position을 폴링한다 — 폴링 세계의 생존 신호(§1-4b).
// ① WAITING 봇: 폴링 = lastseen 도장 → waiting 타임아웃(30s)에 안 쓸려나가고 줄 유지.
//    (이거 없으면 봇들이 30초 만에 전부 evict → 대기열이 즉시 증발 — 시연 불가.)
// ② ADMITTED 봇: 자동예매 ON이면 좌석 잡고 예매하고 나감(자리 순환). OFF면 세션 타임아웃까지 점유.
async function botTick() {
  if (botTick._busy) return;   // 재진입 가드 — 봇 많을 때 틱(3s)이 겹쳐 중복 API 레이스 방지
  botTick._busy = true;
  try {
  for (const f of S.fakes) {
    if (f.done) continue;
    const p = await api(`/api/admission/position?movieId=${encodeURIComponent(S.simMovieId)}&requestId=${encodeURIComponent(f.rid)}`);
    if (!p.ok || !p.data) continue;
    if (p.data.status === 'EXPIRED') { f.done = true; continue; }   // 타임아웃·완료로 큐에서 빠짐
    if (p.data.status !== 'ADMITTED') continue;    // WAITING: 도장 찍었으니 줄 유지, 끝
    if (!$('simBot').checked) continue;            // 자동예매 OFF: admitted인 채 자리 점유(세션 타임아웃까지)
    const sc = await api(`/api/screenings?movieId=${encodeURIComponent(S.simMovieId)}&requestId=${encodeURIComponent(f.rid)}`);
    if (!sc.ok || !sc.data) continue;              // admitted 직후 Kafka 갭이면 403 → 다음 틱 재시도(§2-7)
    const avail = sc.data.find(s => s.remain > 0); if (!avail) { f.done = true; continue; }
    const seats = await api(`/api/seats?screeningId=${avail.screeningId}&requestId=${encodeURIComponent(f.rid)}`);
    const free = (seats.data || []).find(s => !s.taken); if (!free) continue;
    const sel = await api('/api/seats/select', { method: 'POST', body: { screeningId: avail.screeningId, seatNos: [free.seatNo], requestId: f.rid } });
    if (!sel.ok) continue;
    const bk = await api('/api/bookings', { method: 'POST', body: { screeningId: avail.screeningId, seatNos: [free.seatNo], requestId: f.rid, idempotencyKey: `${f.rid}:${avail.screeningId}:${free.seatNo}` } });
    if (!bk.ok) {
      // 예매 실패 — 잡은 좌석·자리 반납(release+leave 짝) 후 종료. 안 하면 TTL까지 유령 점유.
      await api('/api/seats/release', { method: 'POST', body: { screeningId: avail.screeningId, seatNos: [free.seatNo], requestId: f.rid } });
      await api('/api/admission/leave', { method: 'POST', body: { movieId: S.simMovieId, requestId: f.rid } });
    }
    f.done = true;                                  // 성공=자리 반납(닫힌 순환) / 실패=위에서 반납 후 종료
  }
  simStatus();
  } finally { botTick._busy = false; }
}
async function simClear() {
  for (const f of S.fakes) { if (!f.done) api('/api/admission/leave', { method: 'POST', body: { movieId: S.simMovieId, requestId: f.rid } }); }
  S.fakes = []; clearInterval(S.simTimer); S.simTimer = null; simStatus(); toast('가짜 대기자 정리됨');
}

// ================= 바인딩 =================
$('homeBtn').onclick = goHome;
$('waitLeave').onclick = goHome;
$('seatsBack').onclick = backFromSeats;
$('toPay').onclick = toPay;
$('payBack').onclick = () => openSeats(S.screeningId, S.screeningLabel, true);   // 내 좌석 유지(F2)
$('payConfirm').onclick = confirmPay;
$('doneHome').onclick = () => { resetSession(); loadMovies(); };
$('simToggle').onclick = () => { const s = $('sim'); s.classList.toggle('collapsed'); $('simCaret').textContent = s.classList.contains('collapsed') ? '▲' : '▼'; };
$('simAdd').onclick = simAdd;
$('simClear').onclick = simClear;
$('simBot').onchange = simStatus;

// ================= 시작: 로그인 복구 =================
(function init() {
  try { const a = JSON.parse(localStorage.getItem('cgv_auth') || 'null'); if (a && a.rid) { S.rid = a.rid; S.userLabel = a.label; } } catch {}
  renderAuth();
  loadMovies();
  // 새로고침 복구 — 대기/입장 진행 중이었으면 같은 requestId로 enter 재호출.
  // 서버: active=ALREADY_ACTIVE(유지) / waiting=꼬리로 밀림(§1-1). 로그인 상태에서만.
  try {
    const flow = JSON.parse(localStorage.getItem('cgv_flow') || 'null');
    if (S.rid && flow && flow.movieId) enter(flow.movieId, flow.title);
  } catch {}
})();
