#!/usr/bin/env bash
# CGV 풀스택 E2E v2 — 해시태그 키 정확히. 깨끗한 슬레이트.
GW=http://localhost:8088
M=movie-topgun2
R=cgv-onprem_redis_1
out=/mnt/c/Users/Lethe/Desktop/loadmap/cgv-onprem/e2e2.txt
: > "$out"
say(){ echo "=== $* ===" >> "$out"; }
post(){ curl -s -o /tmp/b -w '%{http_code}' -XPOST "$GW$1" -H 'Content-Type: application/json' -d "$2"; echo " | $(cat /tmp/b)"; }
act(){ docker exec $R redis-cli ZRANGE "sessions:{$M}:active" 0 -1 | tr '\n' ' '; }
wai(){ docker exec $R redis-cli ZRANGE "sessions:{$M}:waiting" 0 -1 | tr '\n' ' '; }
adm(){ docker exec $R redis-cli SMEMBERS "admitted:{$M}" | tr '\n' ' '; }

docker exec $R redis-cli FLUSHALL >/dev/null
say "0. clean slate"

say "1. enter a1 (expect 200 ADMITTED)"
post /api/admission/enter '{"movieId":"'$M'","requestId":"a1"}' >> "$out"
say "2. enter a2 (expect 200 ADMITTED)"
post /api/admission/enter '{"movieId":"'$M'","requestId":"a2"}' >> "$out"
say "3. enter a3 (expect 202 WAITING rank1)"
post /api/admission/enter '{"movieId":"'$M'","requestId":"a3"}' >> "$out"

sleep 2
say "4. state after enter (expect active=a1 a2 / waiting=a3 / admitted=a1 a2)"
echo "active  = $(act)" >> "$out"
echo "waiting = $(wai)" >> "$out"
echo "admitted= $(adm)" >> "$out"

say "5. a1 seat select A1,A2 (expect 200 LOCKED)"
post /api/seats/select '{"movieId":"'$M'","theaterId":"T1","seatIds":["A1","A2"],"requestId":"a1"}' >> "$out"
say "6. a3 seat select A3 (expect 403 not admitted)"
post /api/seats/select '{"movieId":"'$M'","theaterId":"T1","seatIds":["A3"],"requestId":"a3"}' >> "$out"
say "7. a1 booking confirm (expect 200 COMPLETED)"
post /api/bookings '{"movieId":"'$M'","theaterId":"T1","seatIds":["A1","A2"],"requestId":"a1"}' >> "$out"

sleep 3
say "8. state after completion (expect active=a2 a3 / waiting empty / admitted=a2 a3)"
echo "active  = $(act)" >> "$out"
echo "waiting = $(wai)" >> "$out"
echo "admitted= $(adm)" >> "$out"
say "9. a3 seat select B1 now (expect 200 LOCKED — promoted+admitted)"
post /api/seats/select '{"movieId":"'$M'","theaterId":"T1","seatIds":["B1"],"requestId":"a3"}' >> "$out"
echo "DONE" >> "$out"
