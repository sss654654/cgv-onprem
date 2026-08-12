// 공용 기반 — 상태·저장소·DOM 유틸·API 래퍼·포맷·실황 출력·오픈 게이트 판정.
// 다른 모듈은 전부 이 파일을 향하고, 이 파일은 아무것도 향하지 않는다(의존 방향 단일).

export const PRICE = 6000;
export const SESSION_SEC = 60;   // 서버 SESSION_TIMEOUT과 정합(데모 1분). 카운트다운 표시용.

// 한 탭이 동시에 유지할 수 있는 가상 관객 수. 브라우저 생성기의 한계이지 백엔드 정원이 아니다
// — 동일 오리진 동시연결 6개로 생존 창(30초) 안에 전원을 폴링할 수 있는 규모.
export const MAX_FAKES = 200;
export const INJECT_PRESETS = [5, 10, 30, 50, 100, 200];
export const RUSH_PRESETS = [0, 10, 50, 100, 200];

export const OPEN_WINDOW_MS = 10 * 60 * 1000;   // 오픈 후 예매 가능 창 = 10분
const CLOSED_SHOW_MS = 60 * 1000;               // 마감 표시를 유지하는 시간
const RUSH_FRESH_MS = 120 * 1000;               // 오픈 직후 이 시간 안에 발견해야 러시를 쏜다
const ROSTER_TTL_MS = 90 * 1000;                // 이보다 묵은 명단은 서버가 이미 회수했다

// ================= 상태 =================
export const S = {
  rid: null, userLabel: null,                 // 신원(requestId + 표시 라벨)
  movieId: null, movieTitle: null,
  pollTimer: null, active: false,             // 폴링 타이머 + 입장(active) 여부
  screeningId: null, screeningLabel: null,
  mine: new Set(), seatPollTimer: null,
  activeUntil: 0, timerTick: null,            // active 카운트다운
  simMovieId: null, fakes: [], simTimer: null,// 시뮬레이터
  lastPollAt: 0,                              // 마지막 폴링 시각(생존 신호 표시)
  pollEpoch: 0, simGen: 0, resumeUntil: 0,    // 폴링 세대(이탈 후 유령 응답 폐기)·시뮬 세대(초기화 취소)·세션 복원
  myRids: new Set(), lastEventId: 0,          // 내 신원들(서버 피드 중복 방지)·서버 실황 커서
};

// ================= 저장소 =================
// 키를 한 곳에 모은다 — 문자열을 여기저기 적으면 지우는 곳과 쓰는 곳이 어긋난다.
export const K = {
  AUTH: 'cgv_auth', FLOW: 'cgv_flow', ROSTER: 'cgv_roster',
  CONSOLE: 'cgv_console', INTRO_CLOSED: 'cgv_intro_closed',
  OPEN_AT: 'cgv_open_at_ms', OPEN_N: 'cgv_open_n', OPEN_FIRED: 'cgv_open_fired',
  OPEN_SEEDED: 'cgv_open_seeded', OPEN_HISTORY: 'cgv_open_history',
};
export const store = {
  get: (k) => localStorage.getItem(k),
  set: (k, v) => localStorage.setItem(k, String(v)),
  del: (...ks) => ks.forEach(k => localStorage.removeItem(k)),
  getJSON(k, fallback = null) { try { return JSON.parse(localStorage.getItem(k) || 'null') ?? fallback; } catch { return fallback; } },
  setJSON(k, v) { localStorage.setItem(k, JSON.stringify(v)); },
};

// ================= DOM =================
export const $ = (id) => document.getElementById(id);
const SCREENS = ['movies', 'waiting', 'entering', 'screenings', 'seats', 'pay', 'done'];
const ACTIVE_SCREENS = ['entering', 'screenings', 'seats', 'pay'];   // 세션 카운트다운이 보일 화면

export function show(name) {
  SCREENS.forEach(s => $('screen-' + s).classList.toggle('hidden', s !== name));
  $('activeTimer').classList.toggle('hidden', !(ACTIVE_SCREENS.includes(name) && S.activeUntil));
  // 화면에 따라 도는 폴링이 달라진다(live.js) — 돌아온 화면이 3초를 기다리지 않게 알린다.
  document.dispatchEvent(new CustomEvent('screen:changed', { detail: name }));
}
export const onScreen = (name) => !$('screen-' + name).classList.contains('hidden');

export function toast(msg, err) {
  const t = $('toast');
  t.textContent = msg;
  t.className = 'toast' + (err ? ' err' : '');
  clearTimeout(toast._t);
  toast._t = setTimeout(() => t.classList.add('hidden'), 2400);
}

// ================= API =================
export async function api(path, opts = {}) {
  // fetch 자체 예외(커넥션 거부·순단 — 백엔드 재시작 중 흔하다)도 잡는다.
  // 안 잡으면 폴링이 throw로 죽어 재예약 없이 영구 정지하고, 화면이 조용히 멈춘다.
  try {
    const res = await fetch(path, {
      method: opts.method || 'GET',
      headers: opts.body ? { 'Content-Type': 'application/json' } : undefined,
      body: opts.body ? JSON.stringify(opts.body) : undefined,
    });
    let data = null; try { data = await res.json(); } catch {}
    return { ok: res.ok, status: res.status, data };
  } catch {
    return { ok: false, status: 0, data: null };   // status 0 = 네트워크 실패. 호출부가 재시도를 판단한다.
  }
}
export const uuid = () => (crypto.randomUUID ? crypto.randomUUID() : 'u' + Date.now() + Math.random().toString(16).slice(2));
export const sleep = (ms) => new Promise(r => setTimeout(r, ms));
export const esc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

// 좌석 락 반환·대기열 이탈 — 여러 경로가 같은 짝 연산을 부르므로 한 곳에 둔다.
// 락을 안 돌려주면 TTL(45초)까지 유령 점유가 남는다.
export const releaseSeats = (screeningId, seatNos, rid) =>
  api('/api/seats/release', { method: 'POST', body: { screeningId, seatNos, requestId: rid } });
export const leaveQueue = (movieId, rid) =>
  api('/api/admission/leave', { method: 'POST', body: { movieId, requestId: rid } });

// ================= 포맷 =================
export function fmtEta(sec) {
  if (sec == null || sec < 0) return '계산 중';
  if (sec < 60) return `${sec}초`;
  return `${Math.floor(sec / 60)}분 ${sec % 60}초`;
}
export function fmtHMS(ms) {
  const s = Math.ceil(ms / 1000);
  const p = (n) => String(n).padStart(2, '0');
  return `${p(Math.floor(s / 3600))}:${p(Math.floor(s % 3600 / 60))}:${p(s % 60)}`;
}
export function fmtAt(ms) {
  const d = new Date(ms);
  const p = (n) => String(n).padStart(2, '0');
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

// ================= 실황 출력 =================
export function feedLog(msg, cls, tMs) {
  const ul = $('feedList'); if (!ul) return;
  const t = new Date(tMs || Date.now()).toTimeString().slice(0, 8);
  const empty = ul.querySelector('.qv-empty'); if (empty) empty.remove();
  // 새 항목만 prepend — 전체를 다시 그리지 않는다. 새 줄에만 슬라이드 인이 걸린다.
  const li = document.createElement('li');
  li.className = cls || '';
  li.innerHTML = `<span>${t}</span>${esc(msg)}`;
  ul.prepend(li);
  while (ul.children.length > 60) ul.lastChild.remove();
}
export function renderFeed() {
  const ul = $('feedList'); if (!ul) return;
  if (!ul.children.length) ul.innerHTML = '<li class="qv-empty">시스템 이벤트(승격·예매·회수)가 여기에 흐릅니다</li>';
}

// ================= 가상 관객 집계 =================
const LIVE_STATES = ['lobby', 'waiting', 'admitted'];
export const activeFakes = () => S.fakes.filter(f => LIVE_STATES.includes(f.state));
export const lobbyFakes = () => S.fakes.filter(f => f.state === 'lobby');

// ================= 오픈 게이트 판정 =================
// 서버는 이 시각을 모른다. 게이트는 화면(예매 버튼)만 잠그고, 예약 투입은 이 탭이 수행한다.
// 값이 localStorage라 브라우저마다 독립이다.
export const openAtMs = () => { const v = parseInt(store.get(K.OPEN_AT) || '0'); return v > 0 ? v : 0; };
export const openRushN = () => Math.min(MAX_FAKES, parseInt(store.get(K.OPEN_N) || '0'));
export const openFiredAt = () => store.get(K.OPEN_FIRED);
export const markOpenFired = (at) => store.set(K.OPEN_FIRED, at);
export const clearOpen = () => store.del(K.OPEN_AT, K.OPEN_N, K.OPEN_FIRED);
export const isRushFresh = () => Date.now() - openAtMs() < RUSH_FRESH_MS;

// 상태만 계산한다(부작용 없음): none(예약 없음 — 상시 열림) / before / open / closed / stale(지난 예약).
export function gateState() {
  const at = openAtMs();
  if (!at) return { s: 'none' };
  const now = Date.now();
  if (now < at) return { s: 'before', remain: at - now };
  if (now < at + OPEN_WINDOW_MS) return { s: 'open', remain: at + OPEN_WINDOW_MS - now };
  if (now < at + OPEN_WINDOW_MS + CLOSED_SHOW_MS) return { s: 'closed' };
  return { s: 'stale' };   // 표시 기간까지 지난 예약. 청소는 sweepExpiredGate가 한다
}
// 표시 기간이 끝난 예약을 지운다 — 재방문자가 죽은 '예매 마감' 화면을 만나지 않게.
// 조회(gateState)와 분리한 이유: 조회는 매초 여러 곳에서 불리는데, 그 안에서 저장소를
// 지우면 어느 호출이 지웠는지 추적할 수 없다.
export function sweepExpiredGate() {
  if (gateState().s === 'stale') clearOpen();
}
// 화면·판정이 쓰는 실효 상태. stale은 예약이 없는 것과 같게 취급한다.
export const effectiveGate = () => {
  const st = gateState();
  return st.s === 'stale' ? { s: 'none' } : st;
};

// ================= 명단 보관 =================
// 새로고침 이어받기용. 여는 쪽이 "읽으면서 지우며" 가져가므로 소유자는 자연히 한 페이지뿐이다
// (두 탭이 같은 관객을 몰면 같은 신원으로 예매가 두 번 나간다).
export function saveRoster() {
  if (!S.fakes.length) { store.del(K.ROSTER); return; }
  store.setJSON(K.ROSTER, {
    at: Date.now(),
    fakes: S.fakes.map(f => ({ rid: f.rid, label: f.label, state: f.state, entered: f.entered })),
  });
}
export function takeRoster() {
  const r = store.getJSON(K.ROSTER);
  store.del(K.ROSTER);
  if (!r || !Array.isArray(r.fakes) || !r.fakes.length) return [];
  if (Date.now() - (r.at || 0) > ROSTER_TTL_MS) return [];
  return r.fakes.map(f => ({ rid: String(f.rid), label: String(f.label), state: f.state, entered: !!f.entered }));
}
