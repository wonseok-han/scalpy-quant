# 미장 자동매매 개발 작업 요약 (2026-05-18~19)

## 변경된 파일 (8개)

### 1. `src/scalpy/data/us_stream.py` — 스트림 중복 틱 제거
- KIS가 같은 체결 데이터를 반복 전송하는 문제 대응
- 같은 심볼에서 price+volume이 직전과 동일하면 skip
- 호가(HDFSASP0) 필드 인덱스 수정: `fields[10]=PBID1`, `fields[11]=PASK1`, `fields[12]=VBID1`, `fields[13]=VASK1`

### 2. `src/scalpy/strategy/volume_spike.py` — 전략 전면 개편
- **기존**: 거래량 폭발 + 가격 급등 감지 (1분 캔들 내 0.5% 상승 필요)
- **최종**: 급등 올라타기 전략
  - 거래량 활발 (최근 평균 >= baseline 평균)
  - 현재 캔들 양봉 (close >= open)
  - 직전 캔들 대비 상승 중
  - 거래가 있는 상태
- 눌림목 전략은 소형주 특성상 패턴이 안 나타나서 폐기
- 디버그 로그 추가 (bearish, low_vol 등 reject reason)

### 3. `src/scalpy/screening/us_screener.py` — 스크리닝 강화
- **일목균형 필터 추가**: `ichimoku_filter=True`일 때 구름 아래 종목 제외, 구름 위 종목 보너스
- **`_ichimoku_cloud_position()`**: 일봉 52개로 구름 위치 판정 (국장과 동일 로직)
- **음전/횡보 종목 제외**: `change_rate < 3%` 종목 스크리닝에서 제외
- **ETP 필터**: DIREXION, PROSHARES 등 레버리지 ETF 종목명 키워드로 제외
- **거래량 급증(volume_surge) 팩터 추가**: 최근 5일 vs 이전 거래량 비율 계산, 랭킹 가중치 30%
- **universe 확대**: 50개 → 150개
- **재스크리닝 주기**: 30분 → 10분

### 4. `src/scalpy/broker/kis_overseas.py` — 주문/취소 API 수정
- **tr_id 수정**:
  - 실전: 매수 `TTTT1002U`, 매도 `TTTT1006U`, 취소 `TTTT1004U`
  - 모의: 매수 `VTTT1002U`, 매도 `VTTT1001U`, 취소 `VTTT1004U`
- **주문 접수를 PENDING으로 처리** (기존 FILLED → PENDING): 체결통보에서 실제 체결 확인
- **OVRS_EXCG_CD**: 변환 없이 `NASD`/`NYSE`/`AMEX` 그대로 사용
- **취소 API 수정**: `PDNO`, `KRX_FWDG_ORD_ORGNO` 필드 추가 (미체결 조회에서 가져옴)
- **가격 항상 전달**: `order.price > 0`이면 지정가 설정
- **에러 로깅 강화**: POST 에러 시 body, msg, msg_cd 로깅

### 5. `src/scalpy/trading/engine.py` — 엔진 안전장치 추가
- **REST API 현재가 조회**: 주문 직전 `get_current_price()`로 실제 가격 확인 (해외주식만)
- **가격 가드**: 실시간 현재가 vs 시그널가 괴리 5% 초과 시 주문 차단
- **주문 버퍼**: 현재가 +0.5% (매수) / -0.5% (매도)로 즉시 체결 유도
- **PENDING 포함 포지션 체크**: max_open_positions에 미체결 매수 주문 포함
- **중복 매수 차단**: PENDING 주문 있는 종목 추가 매수 차단
- **PENDING 상태 처리**: 주문 접수 후 체결통보 대기

### 6. `src/scalpy/trading/order.py` — 주문 관리 개선
- **`_submitted` 딕셔너리**: 접수된 미체결 주문을 order_id로 관리
- **`get_pending()`**: 진행 중 + 접수 후 미체결 합산 반환
- **`mark_filled()`/`mark_cancelled()`**: 체결/취소 시 submitted에서 이동

### 7. `src/scalpy/dashboard/us_routes.py` — UI 반영
- 재스크리닝 후 `_state.screening_symbols` 업데이트 (UI 미반영 버그 수정)
- `ichimoku_filter` 자동 활성화 (일목균형 전략 켜져있을 때)
- ETP 필터링 적용
- universe 150개, 재스크리닝 10분

### 8. `config/settings.toml` — 설정 변경
- `spike_ratio`: 3.0 → 1.0

---

## 발견된 핵심 이슈

| 이슈 | 원인 | 해결 |
|------|------|------|
| 매수 시그널 안 나옴 | 조건 5개 동시 충족 너무 빡빡 | 급등 올라타기로 전략 전환 |
| stale 틱 데이터 | KIS 무료 미국 실시간 ~50% 수준 | REST API 현재가 조회 후 주문 |
| $300에 매수 시도 | 호가 필드 인덱스 잘못 파싱 | KIS 문서 기준으로 수정 |
| 주문 즉시 거부 | tr_id 잘못 사용 (JTTT→TTTT) | KIS 문서 확인 후 수정 |
| ETP 거래 불가 | 해외 ETP 거래 미신청 계좌 | 스크리너에서 ETP 제외 |
| 매수 직후 손절 | ask에 매수 → 현재가와 괴리 → 손절 트리거 | 현재가+0.5% 버퍼 방식으로 변경 |
| 미체결 UI 0건 | FILLED로 즉시 처리 | PENDING 상태 도입 |
| max_positions 초과 | PENDING이 포지션에 미포함 | PENDING 매수도 카운트 |

## 미해결/확인 필요

- 미체결 자동 취소가 정상 동작하는지 확인 필요 (cancel_all_orders)
- 체결통보(H0GSCNI0) → on_fill_notice → 포지션 생성 흐름 실거래 검증 필요
- volume_spike 시그널이 가드(5%)에 자주 걸리면 전략 타이밍 개선 필요
- 디버그 로그(volume_spike.reject 등)는 안정화 후 제거 또는 debug 레벨로 변경
