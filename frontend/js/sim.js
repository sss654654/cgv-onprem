// 시뮬레이터 — 가상 관객 엔진과 오픈 게이트.
// 가상 관객은 가짜 requestId로 대기열에 들어간 클라이언트다. 상태 전이(대기→입장→예매→퇴장)는
// 3초 틱의 폴링이 관찰하고, 스트립의 점과 실황이 그 관찰을 그대로 그린다.
// 폴링이 곧 생존 신호라, 틱이 멈추면 대기 중인 관객은 30초 만에 서버가 회수한다.

import {
  S, K, store, $, toast, api, uuid, feedLog, fmtAt, fmtHMS,
  MAX_FAKES, INJECT_PRESETS, RUSH_PRESETS,
  effectiveGate, openAtMs, openRushN, openFiredAt, markOpenFired, clearOpen, isRushFresh,
  activeFakes, lobbyFakes, releaseSeats, leaveQueue, takeRoster, onScreen,
} from './core.js';
import { goHome } from './booking.js';

const CHUNK = 6;        // 동일 오리진 동시연결 한도 안에서의 병렬 폭
const TICK_MS = 3000;   // 가상 관객 폴링 주기
// 한 틱에서 자동 예매에 쓸 수 있는 시간. 나머지는 생존 폴링 몫으로 남긴다 —
//   폴링이 끊기면 서버가 대기자를 회수하므로, 예매보다 폴링이 먼저다.
const BOOK_BUDGET_MS = 1200;

// ================= 스트립 =================
const DOT_CLASS = { lobby: 'd-lobby', waiting: 'd-wait', admitted: 'd-in', booked: 'd-book', gone: 'd-gone' };
const DOT_NAME = {
  lobby: '대기실 — 오픈을 기다립니다',
  waiting: '대기 중',
  admitted: '입장 — 정원 안입니다',
  booked: '예매 성공 — 자리를 반환하고 나갔습니다',
  gone: '빈손 퇴장 — 아무것도 사지 못하고 나갔습니다',
};

export function renderStrip() {
  const box = $('queueStrip'); if (!box) return;
  box.innerHTML = S.fakes.length
    ? S.fakes.map(f => `<i class="d ${DOT_CLASS[f.state]}" title="관객-${f.label} · ${DOT_NAME[f.state]}"></i>`).join('')
    : '<span class="qv-empty">아직 가상 관객이 없습니다 — 위에서 넣어보세요</span>';
  $('qvCount').textContent = activeFakes().length;
}

// ================= 관객 생성·투입 =================
// 생성만 하고 아직 서버로 보내지 않는다(대기실에 모으는 경우가 있다).
function createFakes(n, state) {
  const list = [];
  for (let i = 0; i < n; i++) {
    const rid = 'bot-' + uuid().slice(0, 8);
    const f = { rid, label: rid.slice(4), state, entered: false };
    S.fakes.push(f); list.push(f); S.myRids.add(rid);
  }
  return list;
}

// 실제로 줄을 세운다. 여섯 개씩 병렬로 보내 원거리에서도 투입이 생존 창(30초)을 넘기지 않게 한다.
async function enterFakes(list, label) {
  if (!list.length) return;
  const gen = S.simGen;
  const btn = $('heroAdd');
  const btnText = btn.textContent;
  btn.disabled = true;
  if (!S.simTimer) S.simTimer = setInterval(botTick, TICK_MS);   // 투입 중에도 생존 폴링이 돌게 미리 켠다

  let admitted = 0, waiting = 0, failed = 0;
  feedLog(`${label} ${list.length}명이 줄을 섭니다`, 'sys');
  try {
    for (let i = 0; i < list.length && gen === S.simGen; i += CHUNK) {
      const batch = list.slice(i, i + CHUNK).map(f =>
        api('/api/admission/enter', { method: 'POST', body: { movieId: S.simMovieId, requestId: f.rid } })
          .then(({ data }) => {
            if (gen !== S.simGen) return;
            if (data && (data.status === 'ADMITTED' || data.status === 'ALREADY_ACTIVE')) { f.state = 'admitted'; f.entered = true; admitted++; }
            else if (data && data.status === 'WAITING') { f.state = 'waiting'; f.entered = true; waiting++; }
            else { f.state = 'gone'; failed++; }   // 지연·네트워크 실패를 입장으로 위장하지 않는다
          }));
      await Promise.all(batch);
      btn.textContent = `투입 중 ${Math.min(i + CHUNK, list.length)}/${list.length}`;
      renderStrip();
    }
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
  if (live + n > MAX_FAKES) { toast(`동시 최대 ${MAX_FAKES}명까지입니다 — 지금 ${live}명`, true); return; }

  const gs = effectiveGate().s;
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
  const room = Math.max(0, MAX_FAKES - activeFakes().length);
  const rush = createFakes(Math.min(rushN || 0, room), 'waiting');
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
    for (let i = 0; i < live.length && gen === S.simGen; i += CHUNK) {
      await Promise.all(live.slice(i, i + CHUNK).map(async f => {
        const p = await api(`/api/admission/position?movieId=${encodeURIComponent(S.simMovieId)}&requestId=${encodeURIComponent(f.rid)}`);
        if (gen !== S.simGen || !p.ok || !p.data) return;
        if (p.data.status === 'EXPIRED') { f.state = 'gone'; feedLog(`관객-${f.label} 퇴장 — 세션 만료·이탈`, 'gone'); return; }
        if (p.data.status !== 'ADMITTED') return;   // 대기 상태면 도장만 찍고 줄을 유지한다
        if (f.state === 'waiting') { f.state = 'admitted'; feedLog(`관객-${f.label} 승격 → 입장`, 'in'); }
      }));
    }

    // ② 자동 예매 여정.
    // 한 명이 네 번 왕복하는데(회차·좌석·선점·예매) 이것을 전원에 대해 순차로 돌면
    //   입장자가 많을 때 틱 하나가 수십 초가 된다. 그동안 ①이 못 돌고, 폴링이 곧 생존
    //   도장이라 서버가 대기자를 전원 회수한다 — 예매를 시키려다 줄을 지우는 셈이다.
    // 그래서 둘을 건다: 청크 단위 병렬로 왕복을 겹치고, 남은 인원은 다음 틱으로 미룬다.
    //   미뤄도 손해가 없다 — 입장 자격은 세션이 살아 있는 동안 유지된다.
    const bookDeadline = Date.now() + BOOK_BUDGET_MS;
    if (canBook) {
      const ready = S.fakes.filter(f => f.state === 'admitted');
      for (let i = 0; i < ready.length && gen === S.simGen; i += CHUNK) {
        if (Date.now() > bookDeadline) break;   // 예산을 넘기면 여기서 끊고 ①을 지킨다
        await Promise.all(ready.slice(i, i + CHUNK).map(async f => {
        if (gen !== S.simGen) return;
        if (f.state !== 'admitted') return;

        const sc = await api(`/api/screenings?movieId=${encodeURIComponent(S.simMovieId)}&requestId=${encodeURIComponent(f.rid)}`);
        if (!sc.ok || !sc.data) return;   // 입장 직후 인증이 아직 없으면 403 — 다음 틱에 다시 시도한다
        const avail = sc.data.find(s => s.remain > 0);
        if (!avail) { f.state = 'gone'; feedLog(`관객-${f.label} 퇴장 — 전 회차 매진`, 'gone'); return; }

        const seats = await api(`/api/seats?screeningId=${encodeURIComponent(avail.screeningId)}&requestId=${encodeURIComponent(f.rid)}`);
        const free = (seats.data || []).find(s => !s.taken);
        if (!free) return;

        const sel = await api('/api/seats/select', { method: 'POST', body: { screeningId: avail.screeningId, seatNos: [free.seatNo], requestId: f.rid } });
        if (!sel.ok) return;

        const bk = await api('/api/bookings', { method: 'POST', body: { screeningId: avail.screeningId, seatNos: [free.seatNo], requestId: f.rid, idempotencyKey: `${f.rid}:${avail.screeningId}:${free.seatNo}` } });
        if (bk.ok) {
          f.state = 'booked';
          feedLog(`관객-${f.label} ${avail.branch} ${avail.screenNo}관 ${free.seatNo} 예매 완료 → 자리 반환`, 'book');
        } else {
          // 실패하면 잡은 좌석과 자리를 돌려주고 끝낸다. 안 그러면 만료까지 점유가 남는다.
          await releaseSeats(avail.screeningId, [free.seatNo], f.rid);
          await leaveQueue(S.simMovieId, f.rid);
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
    if (f.entered && (f.state === 'waiting' || f.state === 'admitted')) { live++; leaveQueue(S.simMovieId, f.rid); }
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
  // 오픈을 기다리지 않고 지금 체험하려는 방문자를 위해 해제 버튼을 잠긴 버튼 옆에 함께 보인다.
  document.querySelectorAll('.gate-skip').forEach(el => el.classList.toggle('hidden', st.s !== 'before'));
  // 대기실은 오픈 예약이 걸렸을 때만 생기는 상태다 — 그 전에는 범례에서도 뺀다.
  const lgLobby = $('lgLobby');
  if (lgLobby) lgLobby.classList.toggle('hidden', st.s !== 'before' && !lobbyFakes().length);

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
  if (sub) sub.textContent = subText;
}

// 발동 — 저장된 표식과 대조해 판단한다. 전이(직전 상태)로 판단하면 새로고침이나
// 백그라운드 탭의 타이머 조임으로 그 순간을 건너뛸 때 아무 일도 일어나지 않는다.
export function fireGateEvents() {
  const st = effectiveGate();
  const at = openAtMs();
  if (!at) return;
  const fired = openFiredAt();

  if (st.s === 'open' && fired !== String(at)) {
    markOpenFired(at);
    feedLog('예매가 열렸습니다 — 입장 시작', 'sys');
    toast('예매가 열렸습니다');
    const rush = openRushN();
    const lobbyN = lobbyFakes().length;
    const fresh = isRushFresh();   // 오픈 한참 뒤에 발견한 예약은 투입을 생략한다
    histPush(at, fresh ? rush + lobbyN : 0);   // 기록은 실제 발동 기준이다
    if (fresh && (rush > 0 || lobbyN > 0)) {
      feedLog(`오픈 — 대기실 ${lobbyN}명과 러시 ${rush}명이 동시에 줄을 섭니다`, 'sys');
      // 즉시 투입이 돌고 있으면 잠시 물러섰다 다시 시도한다 — 러시가 조용히 사라지지 않게.
      const tryRush = (left) => {
        if (!addFakes._busy) releaseLobby(rush);
        else if (left > 0) setTimeout(() => tryRush(left - 1), 2000);
      };
      tryRush(5);
    }
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
function slotOptions() {
  const out = [
    { label: '지금 +1분 (데모)', ms: Date.now() + 60000 },
    { label: '지금 +3분 (데모)', ms: Date.now() + 180000 },
  ];
  const d = new Date();
  d.setSeconds(0, 0);
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
  // 대기열이 도는 장면까지 이어진다. 기다리지 않으려면 카드의 [지금 열기]로 바로 풀 수 있다.
  if (!openAtMs() && !store.get(K.OPEN_SEEDED)) {
    store.set(K.OPEN_SEEDED, '1');
    store.set(K.OPEN_AT, String(slotOptions()[0].ms));
    store.set(K.OPEN_N, '50');
  }

  $('openSet').onclick = () => {
    const at = parseInt(sel.value) || 0;
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
    clearOpen();
    feedLog('오픈 예약을 해제했습니다', 'sys');
    renderGate();
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
  S.fakes.forEach(f => S.myRids.add(f.rid));
  if (activeFakes().length && !S.simTimer) S.simTimer = setInterval(botTick, TICK_MS);
  feedLog(`이전 화면의 가상 관객 ${S.fakes.length}명을 이어받았습니다`, 'sys');
}
