// 서버가 정본인 두 가지를 화면에 붙인다 — 전체 현황(숫자)과 실황(이벤트).
// 둘 다 읽기 전용이고, 어느 방문자가 열어도 같은 값을 본다.
//
// 둘 다 화면에 보이는 동안만 부른다. 대기·좌석·결제 화면으로 넘어가면 이 값들은
// 화면에서 사라지는데, 그때도 계속 부르면 대기 중인 사용자 한 명이 안 보는 데이터를
// 3초마다 두 번씩 받아 간다. 대기 인원이 많을수록 그 몫이 그대로 곱해진다.

import { S, $, api, feedLog, onScreen, setSessionSec } from './core.js';

const POLL_MS = 3000;
const STALE_AFTER = 3;   // 연속 실패가 이만큼 쌓이면 화면 값이 낡았다고 표시한다

// 실황 피드는 시뮬레이터 콘솔 안에 있다 — 접으면 보이지 않는다.
// 콘솔은 screen-movies 안에 있어 다른 화면으로 넘어가면 같이 가려지는데, show()가
//   섹션에만 hidden을 걸고 conBody 자신의 클래스는 안 건드린다. 그래서 conBody만 보면
//   대기 화면에서도 계속 true가 나와 폴링이 멈추지 않았다.
//   대기자 1,000명이면 그 몫이 초당 약 333건이고, 그것도 목록 100건을 통째로 읽는
//   가장 무거운 경로다 — 대기 화면에는 그릴 자리가 없는데 받아 오기만 한다.
// 화면 조건을 같이 본다: 전체 현황과 같은 기준(screen-movies)에 콘솔 펼침을 더한다.
const feedVisible = () => {
  if (!onScreen('movies')) return false;
  const body = $('conBody');
  return !!body && !body.classList.contains('hidden');
};

// ================= 서버 설정 읽기 =================
// 첫 화면에 있던 두 타일("전체 대기"·"입장까지 예상")을 뺐다. 오픈 전에는 줄이 서지 않아
//   대기가 늘 0이고 예상도 "오픈 전"이라, 방문자가 처음 보는 자리에서 둘 다 고정값이었다.
//   같은 숫자를 콘솔의 관객 스트립·실황이 움직이는 채로 보여준다.
// 타일이 사라지면서 3초 폴링의 근거도 없어졌다. 남은 용도는 세션 수명 하나인데,
//   그 값은 배포 때만 바뀌므로 화면을 열 때 한 번 읽으면 된다.
export async function loadServerConfig() {
  if (!S.simMovieId) return;
  const { ok, data } = await api(`/api/admission/stats?movieId=${encodeURIComponent(S.simMovieId)}`);
  if (!ok || !data) return;
  // 세션 수명은 서버가 정한다. 프런트에 상수로 두면 env를 바꿨을 때 화면만 옛 값으로 남는다.
  setSessionSec(data.sessionTimeoutSeconds);
}

// ================= 좌석 현황판 =================
// 입장 전에도 보이는 유일한 "움직이는 숫자"다. 예매 화면의 좌석 목록과 달리 게이트가 없고,
//   좌석 번호 없이 회차별 잔여/전체만 받는다(/api/screenings/board).
const boardVisible = () => onScreen('movies');
let boardPrev = {};
export async function pollBoard() {
  if (!S.simMovieId || !boardVisible()) return;
  const box = $('seatBoard'); if (!box) return;
  const { ok, data } = await api(`/api/screenings/board?movieId=${encodeURIComponent(S.simMovieId)}`);
  if (!ok || !Array.isArray(data) || !data.length) { box.hidden = true; return; }
  box.hidden = false;
  // 한 줄 요약은 늘 보인다 — 좌석이 줄어드는 것을 첫 화면에서 보게 하는 것이 이 판의 목적이다.
  //   회차 12칸을 상시 펼쳐 두면 예매 카드가 그만큼 밀리므로 상세는 접어 둔다.
  const total = data.reduce((a, s) => a + s.total, 0);
  const remain = data.reduce((a, s) => a + s.remain, 0);
  const sum = $('boardSum');
  if (sum) sum.innerHTML = `좌석 <b id="boardRemain">${remain}</b> / ${total} 남음`;
  if (boardPrev.__sum !== undefined && boardPrev.__sum !== remain) {
    const b = $('boardRemain');
    if (b) { b.classList.add('bump'); setTimeout(() => b.classList.remove('bump'), 500); }
  }
  boardPrev.__sum = remain;

  const grid = $('boardGrid');
  grid.innerHTML = data.map(s => {
    const pct = s.total > 0 ? Math.round((s.total - s.remain) / s.total * 100) : 0;
    // 잔여가 줄수록 눈에 띄게 — 매진 임박이 숫자를 읽기 전에 보이게 한다.
    const tone = s.remain === 0 ? 'full' : pct >= 80 ? 'hot' : pct >= 40 ? 'warn' : '';
    return `<div class="bd ${tone}" data-id="${s.screeningId}">
      <span class="bd-name">${s.branch} ${s.screenNo}관</span>
      <span class="bd-num"><b>${s.remain === 0 ? '매진' : s.remain}</b>${s.remain === 0 ? '' : ` / ${s.total}`}</span>
    </div>`;
  }).join('');
  // 값이 바뀐 칸만 깜빡인다 — 러시가 좌석을 깎는 순간이 보이게.
  data.forEach(s => {
    if (boardPrev[s.screeningId] !== undefined && boardPrev[s.screeningId] !== s.remain) {
      const el = grid.querySelector(`[data-id="${s.screeningId}"] b`);
      if (el) { el.classList.add('bump'); setTimeout(() => el.classList.remove('bump'), 500); }
    }
    boardPrev[s.screeningId] = s.remain;
  });
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

let feedFails = 0;

export async function pollEvents() {
  if (!S.simMovieId || !feedVisible()) return;
  const { ok, data } = await api(`/api/admission/events?movieId=${encodeURIComponent(S.simMovieId)}&after=${S.lastEventId}`);
  const list = $('feedList');
  // 현황 막대와 같은 규칙으로 낡음을 드러낸다 — 실황이 조용히 멈추면 방문자가 알 방법이 없다.
  if (!ok || !data) {
    feedFails += 1;
    if (list && feedFails >= STALE_AFTER) list.classList.add('stale');
    return;
  }
  feedFails = 0;
  if (list) list.classList.remove('stale');
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
  setInterval(pollEvents, POLL_MS);
  setInterval(pollBoard, POLL_MS);
  document.addEventListener('console:toggled', (e) => { if (e.detail) pollEvents(); });
  // 다시 보이게 된 순간에 한 번 당겨 온다 — 다음 틱까지 낡은 값이 남지 않게.
  document.addEventListener('screen:changed', () => { if (boardVisible()) pollBoard(); });
}
