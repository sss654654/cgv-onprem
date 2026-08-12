// 서버가 정본인 두 가지를 화면에 붙인다 — 전체 현황(숫자)과 실황(이벤트).
// 둘 다 읽기 전용이고, 어느 방문자가 열어도 같은 값을 본다.
//
// 둘 다 화면에 보이는 동안만 부른다. 대기·좌석·결제 화면으로 넘어가면 이 값들은
// 화면에서 사라지는데, 그때도 계속 부르면 대기 중인 사용자 한 명이 안 보는 데이터를
// 3초마다 두 번씩 받아 간다. 대기 인원이 많을수록 그 몫이 그대로 곱해진다.

import { S, $, api, fmtEta, feedLog, effectiveGate, onScreen } from './core.js';

const POLL_MS = 3000;
const STALE_AFTER = 3;   // 연속 실패가 이만큼 쌓이면 화면 값이 낡았다고 표시한다

let fails = 0;
let prevVals = [];

// 전체 현황 타일은 screen-movies 안에 있다 — 다른 화면에서는 그릴 자리가 없다.
const statsVisible = () => onScreen('movies');
// 실황 피드는 시뮬레이터 콘솔 안에 있다 — 접으면 보이지 않는다.
const feedVisible = () => {
  const body = $('conBody');
  return !!body && !body.classList.contains('hidden');
};

// ================= 전체 현황 =================
export async function pollStats() {
  if (!S.simMovieId || !statsVisible()) return;
  const { ok, data } = await api(`/api/admission/stats?movieId=${encodeURIComponent(S.simMovieId)}`);
  const bar = $('statsBar');
  if (!ok || !data) {
    // 직전 값을 유지한다 — 3초마다 지웠다 그리면 화면이 깜빡인다.
    // 다만 연속 실패가 이어지면 흐리게 해 낡은 값임을 드러낸다.
    fails += 1;
    if (bar && fails >= STALE_AFTER) bar.classList.add('stale');
    return;
  }
  fails = 0;
  if (bar) bar.classList.remove('stale');

  // 정원(capacity)은 화면에 내보내지 않는다 — 실서비스가 정원을 광고하지 않는 것과 같게.
  // "바로 입장" 판정에만 쓴다. 게이트 상태가 우선한다(오픈 전·마감에는 입장 자체가 안 된다).
  const gs = effectiveGate().s;
  const canEnterNow = data.active < data.capacity && data.waiting === 0 && (gs === 'none' || gs === 'open');
  let eta;
  if (gs === 'before') eta = '오픈 전';
  else if (gs === 'closed') eta = '마감';
  else eta = canEnterNow ? '바로 입장' : fmtEta(data.etaNextSeconds ?? -1);

  // 이 타일은 서버가 아는 숫자만 싣는다. 대기실은 오픈 순간까지 이 브라우저가 들고 있는
  // 시뮬레이터 상태라 서버 대기열과 섞으면 같은 자리에서 두 세계가 뒤섞인다 — 그쪽은 콘솔이 맡는다.
  // 오픈 전 대기열이 0인 것은 정상이다: 줄은 오픈 순간에 선다.
  const vals = [['전체 대기', data.waiting], ['입장까지 예상', eta]];
  if (!bar) return;

  bar.innerHTML = vals.map(([k, v]) => `<span class="stat"><em>${k}</em><b>${v}</b></span>`).join('');
  // 값이 바뀐 타일에만 펄스 — 화면이 살아 있다는 신호
  vals.forEach(([, v], i) => {
    if (prevVals[i] !== undefined && prevVals[i] !== v) {
      const b = bar.children[i] && bar.children[i].querySelector('b');
      if (b) { b.classList.add('bump'); setTimeout(() => b.classList.remove('bump'), 500); }
    }
  });
  prevVals = vals.map(([, v]) => v);
}

// ================= 실황 =================
// 실황의 정본은 서버(queue의 이벤트 리스트)다 — 새로고침·새 탭·다른 기기 어디서든 같은 내용이
// 흐른다. 내 신원의 이벤트는 건너뛴다: 내 관찰(좌석까지 담은 더 자세한 줄)이 이미 그리고 있어서,
// 서버 줄까지 그리면 같은 일이 두 번 찍힌다.
const EVENT_TEXT = {
  ADMIT:          ['입장 — 정원 여유', 'in'],
  PROMOTE:        ['승격 → 입장', 'in'],
  SESSION_EXPIRE: ['퇴장 — 세션 만료', 'gone'],
  WAIT_EXPIRE:    ['퇴장 — 자리 비움', 'gone'],
  LEAVE:          ['이탈', 'gone'],
  RETURN:         ['자리 반환', 'book'],
};

export async function pollEvents() {
  if (!S.simMovieId || !feedVisible()) return;
  const { ok, data } = await api(`/api/admission/events?movieId=${encodeURIComponent(S.simMovieId)}&after=${S.lastEventId}`);
  if (!ok || !data) return;
  // 데이터 초기화로 서버 피드가 비면 id가 뒤로 간다 — 커서를 버리고 처음부터 다시 받는다.
  if ((data.last || 0) < S.lastEventId) S.lastEventId = 0;
  (data.events || []).forEach(e => {
    if (e.id > S.lastEventId) S.lastEventId = e.id;
    if (S.myRids.has(e.rid)) return;
    const text = EVENT_TEXT[e.type];
    if (!text) return;
    const label = '관객-' + String(e.rid).replace(/^bot-/, '').slice(0, 8);
    feedLog(`${label} ${text[0]}`, text[1], e.t);
  });
}

export function startLivePolling() {
  setInterval(pollStats, POLL_MS);
  setInterval(pollEvents, POLL_MS);
  // 다시 보이게 된 순간에 한 번 당겨 온다 — 다음 틱까지 3초 동안 낡은 값이 남지 않게.
  document.addEventListener('screen:changed', () => { if (statsVisible()) pollStats(); });
  document.addEventListener('console:toggled', (e) => { if (e.detail) pollEvents(); });
}
